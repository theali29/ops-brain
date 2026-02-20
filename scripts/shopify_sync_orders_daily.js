"use strict";

const path = require("path");
const axios = require("axios");
require("dotenv").config({ path: path.resolve(__dirname, "../.env") });

const { pool } = require("../config/db");


const SHOP = (process.env.SHOPIFY_STORE || "").trim(); 
const TOKEN = (process.env.SHOPIFY_ADMIN_TOKEN || "").trim();
const VERSION = (process.env.SHOPIFY_API_VERSION || "2024-10").trim();

const PIPELINE = "shopify_orders_daily";
const STATE_KEY = "shopify_orders_daily";

const LIMIT = 250;
const RPS = Number(process.env.SHOPIFY_RPS || 2); 
const INSERT_STUB_SKUS = (process.env.INSERT_STUB_SKUS || "true").toLowerCase() !== "false";

const DEFAULT_LOOKBACK_DAYS = Number(process.env.DEFAULT_LOOKBACK_DAYS || 7);
const OVERLAP_HOURS = Number(process.env.OVERLAP_HOURS || 48);

const LOG_EVERY_ORDERS = Number(process.env.LOG_EVERY_ORDERS || 1000);


if (!SHOP || !TOKEN) {
  console.error("Missing SHOPIFY_STORE or SHOPIFY_ADMIN_TOKEN in .env");
  process.exit(1);
}
if (!Number.isFinite(RPS) || RPS <= 0) {
  console.error("SHOPIFY_RPS must be a positive number");
  process.exit(1);
}
if (!Number.isFinite(OVERLAP_HOURS) || OVERLAP_HOURS < 0) {
  console.error("OVERLAP_HOURS must be a non-negative number");
  process.exit(1);
}


const api = axios.create({
  baseURL: `https://${SHOP}/admin/api/${VERSION}`,
  headers: {
    "X-Shopify-Access-Token": TOKEN,
    "Content-Type": "application/json",
  },
  timeout: 60000,
  maxRedirects: 5,
  validateStatus: () => true,
});


function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function nowIso() {
  return new Date().toISOString();
}

function isoDaysAgo(days) {
  const d = new Date(Date.now() - days * 86400 * 1000);
  return d.toISOString();
}

function isoHoursAgoFrom(iso, hours) {
  const base = new Date(iso);
  const d = new Date(base.getTime() - hours * 3600 * 1000);
  return d.toISOString();
}

function parseTags(tags) {
  if (!tags) return [];
  if (Array.isArray(tags)) return tags;
  return String(tags)
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean);
}

function isSubscriptionFromTags(tagsArr) {
  const t = (tagsArr || []).map((x) => String(x).toLowerCase());
  return (
    t.includes("subscription") ||
    t.includes("subscription first order") ||
    t.includes("recurring order") ||
    t.some((x) => x.includes("subscription")) ||
    t.some((x) => x.includes("loop")) ||
    t.some((x) => x.includes("recurring"))
  );
}

function moneyToNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}

function sumTaxLines(taxLines) {
  if (!Array.isArray(taxLines)) return 0;
  return taxLines.reduce((acc, tl) => acc + moneyToNum(tl.price), 0);
}

/**
 * Shopify REST cursor pagination uses Link header:
 * <https://.../orders.json?...&page_info=XYZ>; rel="next"
 */
function extractNextPageInfo(linkHeader) {
  if (!linkHeader) return null;

  const parts = linkHeader.split(",");
  for (const p of parts) {
    const section = p.trim();
    if (!section.includes('rel="next"')) continue;

    const m = section.match(/<([^>]+)>/);
    if (!m) continue;

    const url = m[1];
    try {
      const u = new URL(url);
      return u.searchParams.get("page_info");
    } catch {
      return null;
    }
  }
  return null;
}

let lastRequestAt = 0;

async function requestWithRetry(method, url, { params, data } = {}, opts = {}) {
  const maxAttempts = opts.maxAttempts ?? 8;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const minGapMs = Math.ceil(1000 / RPS);
    const elapsed = Date.now() - lastRequestAt;
    if (elapsed < minGapMs) await sleep(minGapMs - elapsed);
    lastRequestAt = Date.now();

    let res;
    try {
      res = await api.request({ method, url, params, data });
    } catch (err) {
      const backoff = Math.min(30000, 500 * 2 ** (attempt - 1));
      if (attempt === maxAttempts) throw err;
      await sleep(backoff);
      continue;
    }

    if (res.status >= 200 && res.status < 300) return res;

    // Rate limit
    if (res.status === 429) {
      const retryAfter = Number(res.headers["retry-after"] || 1);
      const waitMs = Math.max(1000, retryAfter * 1000);
      await sleep(waitMs);
      continue;
    }

    // Transient server errors
    if (res.status >= 500 && res.status <= 599) {
      const backoff = Math.min(30000, 500 * 2 ** (attempt - 1));
      await sleep(backoff);
      continue;
    }

    // Non-retryable
    const body = typeof res.data === "string" ? res.data : JSON.stringify(res.data);
    throw new Error(`HTTP ${res.status}: ${body}`);
  }

  throw new Error(`Request failed after ${maxAttempts} attempts`);
}


async function getSyncCursor(key) {
  const res = await pool.query("select value from ops.sync_state where key = $1", [key]);
  if (!res.rowCount) return null;
  return res.rows[0].value;
}

async function setSyncCursor(key, value) {
  const q = `
    insert into ops.sync_state (key, value, updated_at)
    values ($1, $2::jsonb, now())
    on conflict (key) do update set
      value = excluded.value,
      updated_at = now()
  `;
  await pool.query(q, [key, JSON.stringify(value)]);
}

async function startIngestionRun(metadata) {
  const q = `
    insert into ops.ingestion_runs (source, pipeline, status, metadata)
    values ('shopify', $1, 'started', $2::jsonb)
    returning id
  `;
  const res = await pool.query(q, [PIPELINE, JSON.stringify(metadata || {})]);
  return res.rows[0].id;
}

async function finishIngestionRun(runId, status, rowsUpserted, rowsDeleted, errorMessage, metadataPatch) {
  const q = `
    update ops.ingestion_runs
    set finished_at = now(),
        status = $2::ops.sync_status,
        rows_upserted = $3,
        rows_deleted = $4,
        error_message = $5,
        metadata = metadata || $6::jsonb
    where id = $1
  `;
  await pool.query(q, [
    runId,
    status,
    rowsUpserted || 0,
    rowsDeleted || 0,
    errorMessage || null,
    JSON.stringify(metadataPatch || {}),
  ]);
}

async function upsertOrder(client, order) {
  const tagsArr = parseTags(order.tags);

  const q = `
    insert into ops.fact_shopify_orders
      (order_id, order_number, customer_id, processed_at, created_at_shopify,
       currency, financial_status, fulfillment_status, cancelled_at, cancel_reason,
       tags, is_test, raw, ingested_at)
    values
      ($1,$2,$3,$4,$5,
       $6,$7,$8,$9,$10,
       $11,$12,$13::jsonb, now())
    on conflict (order_id) do update set
      order_number = excluded.order_number,
      customer_id = excluded.customer_id,
      processed_at = excluded.processed_at,
      created_at_shopify = excluded.created_at_shopify,
      currency = excluded.currency,
      financial_status = excluded.financial_status,
      fulfillment_status = excluded.fulfillment_status,
      cancelled_at = excluded.cancelled_at,
      cancel_reason = excluded.cancel_reason,
      tags = excluded.tags,
      is_test = excluded.is_test,
      raw = excluded.raw,
      ingested_at = now()
  `;

  await client.query(q, [
    order.id,
    order.order_number != null ? String(order.order_number) : null,
    order.customer?.id ?? null,
    order.processed_at ?? null,
    order.created_at ?? null,
    order.currency ?? null,
    order.financial_status ?? null,
    order.fulfillment_status ?? null,
    order.cancelled_at ?? null,
    order.cancel_reason ?? null,
    tagsArr,
    !!order.test,
    JSON.stringify(order),
  ]);

  return tagsArr;
}

async function ensureSkuExists(client, sku, { productTitle, variantTitle, productId, variantId } = {}) {
  const check = await client.query("select 1 from ops.dim_sku where sku = $1 limit 1", [sku]);
  if (check.rowCount) return false;

  if (!INSERT_STUB_SKUS) {
    throw new Error(`SKU not found in ops.dim_sku and INSERT_STUB_SKUS=false: ${sku}`);
  }

  const q = `
    insert into ops.dim_sku
      (sku, product_title, variant_title, shopify_product_id, shopify_variant_id, attributes, created_at, updated_at)
    values
      ($1,  $2,           $3,            $4,               $5,                $6::jsonb, now(), now())
    on conflict (sku) do nothing
  `;

  const attributes = {
    stub: true,
    stub_reason: "seen_in_order_line_item_daily_sync",
    created_by: PIPELINE,
    created_at: nowIso(),
  };

  await client.query(q, [
    sku,
    productTitle ?? null,
    variantTitle ?? null,
    productId ?? null,
    variantId ?? null,
    JSON.stringify(attributes),
  ]);

  return true;
}

async function upsertLineItem(client, orderId, orderTagsArr, li) {
  const sku = (li.sku || "").trim();
  if (!sku) return { inserted: false, skippedNoSku: true, stubInserted: false };

  const tagsSub = isSubscriptionFromTags(orderTagsArr);

  const qty = li.quantity || 0;
  const gross = moneyToNum(li.price) * qty;
  const discount = moneyToNum(li.total_discount);
  const tax = sumTaxLines(li.tax_lines);
  const net = gross - discount;

  const stubInserted = await ensureSkuExists(client, sku, {
    productTitle: li.title ?? null,
    variantTitle: li.variant_title ?? li.name ?? null,
    productId: li.product_id ?? null,
    variantId: li.variant_id ?? null,
  });

  const q = `
    insert into ops.fact_shopify_order_line_items
      (order_id, line_item_id, sku, shopify_product_id, shopify_variant_id,
       shopify_inventory_item_id, title, variant_title, quantity, fulfillment_service,
       gross_item_revenue, discount_amount, tax_amount, net_item_revenue,
       is_subscription, source, ingested_at)
    values
      ($1,$2,$3,$4,$5,
       $6,$7,$8,$9,$10,
       $11,$12,$13,$14,
       $15,'shopify', now())
    on conflict (order_id, line_item_id) do update set
      sku = excluded.sku,
      shopify_product_id = excluded.shopify_product_id,
      shopify_variant_id = excluded.shopify_variant_id,
      shopify_inventory_item_id = excluded.shopify_inventory_item_id,
      title = excluded.title,
      variant_title = excluded.variant_title,
      quantity = excluded.quantity,
      fulfillment_service = excluded.fulfillment_service,
      gross_item_revenue = excluded.gross_item_revenue,
      discount_amount = excluded.discount_amount,
      tax_amount = excluded.tax_amount,
      net_item_revenue = excluded.net_item_revenue,
      is_subscription = excluded.is_subscription,
      ingested_at = now()
  `;

  await client.query(q, [
    orderId,
    li.id,
    sku,
    li.product_id ?? null,
    li.variant_id ?? null,
    null, // inventory_item_id not provided on REST line_item
    li.title ?? null,
    li.variant_title ?? null,
    qty,
    li.fulfillment_service ?? null,
    gross,
    discount,
    tax,
    net,
    tagsSub,
  ]);

  return { inserted: true, skippedNoSku: false, stubInserted };
}

async function upsertRefunds(client, order) {
  const refunds = Array.isArray(order.refunds) ? order.refunds : [];
  let refundHeaders = 0;
  let refundLines = 0;

  for (const r of refunds) {
    const qh = `
      insert into ops.fact_shopify_refunds
        (refund_id, order_id, created_at_refund, note, source, raw, ingested_at)
      values
        ($1,$2,$3,$4,'shopify',$5::jsonb, now())
      on conflict (refund_id) do update set
        order_id = excluded.order_id,
        created_at_refund = excluded.created_at_refund,
        note = excluded.note,
        raw = excluded.raw,
        ingested_at = now()
    `;
    await client.query(qh, [
      r.id,
      order.id,
      r.created_at ?? null,
      r.note ?? null,
      JSON.stringify(r),
    ]);
    refundHeaders++;

    const rlis = Array.isArray(r.refund_line_items) ? r.refund_line_items : [];
    for (const x of rlis) {
      const li = x.line_item || {};
      const lineItemId = li.id ?? x.line_item_id ?? null;
      const sku = (li.sku || "").trim() || null;

      // If we have SKU, satisfy FK best-effort
      if (sku) {
        await ensureSkuExists(client, sku, {
          productTitle: li.title ?? null,
          variantTitle: li.variant_title ?? null,
          productId: li.product_id ?? null,
          variantId: li.variant_id ?? null,
        });
      }

      const refundAmount =
        moneyToNum(x.subtotal) ||
        moneyToNum(x.amount) ||
        moneyToNum(x.total_tax) ||
        0;

      // Upsert line item if possible (requires unique index on (refund_id, line_item_id))
      // If lineItemId is null, we cannot safely de-dupe; we still insert best-effort.
      if (lineItemId != null) {
        const ql = `
          insert into ops.fact_shopify_refund_line_items
            (refund_id, order_id, line_item_id, sku, quantity, refund_amount, source, ingested_at)
          values
            ($1,$2,$3,$4,$5,$6,'shopify', now())
          on conflict (refund_id, line_item_id) do update set
            sku = excluded.sku,
            quantity = excluded.quantity,
            refund_amount = excluded.refund_amount,
            ingested_at = now()
        `;
        await client.query(ql, [
          r.id,
          order.id,
          lineItemId,
          sku,
          x.quantity ?? 0,
          refundAmount,
        ]);
      } else {
        const ql = `
          insert into ops.fact_shopify_refund_line_items
            (refund_id, order_id, line_item_id, sku, quantity, refund_amount, source, ingested_at)
          values
            ($1,$2,$3,$4,$5,$6,'shopify', now())
        `;
        await client.query(ql, [
          r.id,
          order.id,
          null,
          sku,
          x.quantity ?? 0,
          refundAmount,
        ]);
      }

      refundLines++;
    }
  }

  return { refundHeaders, refundLines };
}

async function main() {
  // Read durable cursor
  const cursor = await getSyncCursor(STATE_KEY);

  // We store: { last_max_updated_at: "..." }
  const lastMax = cursor?.last_max_updated_at || isoDaysAgo(DEFAULT_LOOKBACK_DAYS);

  // Apply overlap (prevents missing edge updates)
  const updatedAtMin = OVERLAP_HOURS > 0 ? isoHoursAgoFrom(lastMax, OVERLAP_HOURS) : lastMax;

  const runMeta = {
    shop: SHOP,
    version: VERSION,
    rps: RPS,
    limit: LIMIT,
    last_max_updated_at: lastMax,
    updated_at_min: updatedAtMin,
    overlap_hours: OVERLAP_HOURS,
    started_at: nowIso(),
  };

  const runId = await startIngestionRun(runMeta);

  console.log(
    `[${PIPELINE}] start | updated_at_min=${updatedAtMin} | overlap_hours=${OVERLAP_HOURS} | rps=${RPS}`
  );

  const client = await pool.connect();

  let ordersUpserted = 0;
  let lineItemsUpserted = 0;
  let lineItemsSkippedNoSku = 0;
  let stubSkusInserted = 0;
  let refundHeadersUpserted = 0;
  let refundLinesUpserted = 0;

  let maxUpdatedAtSeen = lastMax; // persist as watermark only on success
  let pageInfo = null;

  try {
    while (true) {
      const params = pageInfo
        ? {
            limit: LIMIT,
            page_info: pageInfo,
            fields:
              "id,order_number,processed_at,created_at,updated_at,currency,financial_status,fulfillment_status,cancelled_at,cancel_reason,tags,test,customer,line_items,refunds",
          }
        : {
            limit: LIMIT,
            status: "any",
            order: "updated_at asc",
            updated_at_min: updatedAtMin,
            fields:
              "id,order_number,processed_at,created_at,updated_at,currency,financial_status,fulfillment_status,cancelled_at,cancel_reason,tags,test,customer,line_items,refunds",
          };

      const res = await requestWithRetry("GET", "/orders.json", { params });

      if (res.status < 200 || res.status >= 300) {
        throw new Error(`Unexpected status ${res.status}: ${JSON.stringify(res.data)}`);
      }

      const orders = res.data?.orders || [];
      const nextPageInfo = extractNextPageInfo(res.headers?.link);

      if (!orders.length) break;

      for (const o of orders) {
        await client.query("BEGIN");
        try {
          const tagsArr = await upsertOrder(client, o);
          ordersUpserted++;

          const u = o.updated_at || o.processed_at || o.created_at;
          if (u && new Date(u) > new Date(maxUpdatedAtSeen)) {
            maxUpdatedAtSeen = u;
          }

          for (const li of o.line_items || []) {
            const out = await upsertLineItem(client, o.id, tagsArr, li);
            if (out.skippedNoSku) {
              lineItemsSkippedNoSku++;
              continue;
            }
            if (out.stubInserted) stubSkusInserted++;
            lineItemsUpserted++;
          }

          const { refundHeaders, refundLines } = await upsertRefunds(client, o);
          refundHeadersUpserted += refundHeaders;
          refundLinesUpserted += refundLines;

          await client.query("COMMIT");
        } catch (e) {
          await client.query("ROLLBACK");
          throw e;
        }

        if (ordersUpserted % LOG_EVERY_ORDERS === 0) {
          console.log(
            `[${PIPELINE}] progress | orders=${ordersUpserted} line_items=${lineItemsUpserted} refunds=${refundHeadersUpserted}`
          );
        }
      }

      pageInfo = nextPageInfo;
      if (!pageInfo) break;
    }

    // Update checkpoint only on success (durable, no local files)
    await setSyncCursor(STATE_KEY, { last_max_updated_at: maxUpdatedAtSeen });

    const rowsUpserted =
      ordersUpserted +
      lineItemsUpserted +
      stubSkusInserted +
      refundHeadersUpserted +
      refundLinesUpserted;

    await finishIngestionRun(
      runId,
      "succeeded",
      rowsUpserted,
      0,
      null,
      { finished_at: nowIso(), new_cursor: maxUpdatedAtSeen }
    );

    console.log(
      `[${PIPELINE}] ok | orders=${ordersUpserted} line_items=${lineItemsUpserted} refunds=${refundHeadersUpserted} new_cursor=${maxUpdatedAtSeen}`
    );
  } catch (err) {
    const msg = err.response?.data ? JSON.stringify(err.response.data) : err.message;

    const rowsUpserted =
      ordersUpserted +
      lineItemsUpserted +
      stubSkusInserted +
      refundHeadersUpserted +
      refundLinesUpserted;

    await finishIngestionRun(
      runId,
      "failed",
      rowsUpserted,
      0,
      msg,
      { finished_at: nowIso(), cursor_not_advanced: true }
    );

    console.error(`[${PIPELINE}] failed | ${msg}`);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

main();