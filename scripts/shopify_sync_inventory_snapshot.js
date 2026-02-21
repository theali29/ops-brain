"use strict";
const path = require("path");
const axios = require("axios");
require("dotenv").config({ path: path.resolve(__dirname, "../.env") });
const { pool } = require("../config/db");
const SHOP = (process.env.SHOPIFY_STORE || "").trim();
const TOKEN = (process.env.SHOPIFY_ADMIN_TOKEN || "").trim();
const VERSION = (process.env.SHOPIFY_API_VERSION || "2024-10").trim();

const PIPELINE = "shopify_inventory_snapshot";
const LIMIT = 250;
const RPS = Number(process.env.SHOPIFY_RPS || 2);
const SNAPSHOT_DATE = (process.env.SNAPSHOT_DATE || "").trim(); 

const LOCATION_NAME_ALLOWLIST = (process.env.LOCATION_NAME_ALLOWLIST || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);


const INVENTORY_ITEM_ID_CHUNK = 50;

if (!SHOP || !TOKEN) {
  console.error("Missing SHOPIFY_STORE or SHOPIFY_ADMIN_TOKEN in .env");
  process.exit(1);
}
if (!Number.isFinite(RPS) || RPS <= 0) {
  console.error("SHOPIFY_RPS must be a positive number");
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
function todayUtcYYYYMMDD() {
  const d = new Date();
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}
function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

function extractNextPageInfo(linkHeader) {
  if (!linkHeader) return null;
  const parts = linkHeader.split(",");
  for (const p of parts) {
    const section = p.trim();
    if (!section.includes('rel="next"')) continue;
    const m = section.match(/<([^>]+)>/);
    if (!m) continue;
    try {
      const u = new URL(m[1]);
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

    if (res.status === 429) {
      const retryAfter = Number(res.headers?.["retry-after"] || 1);
      await sleep(Math.max(1000, retryAfter * 1000));
      continue;
    }

    if (res.status >= 500 && res.status <= 599) {
      const backoff = Math.min(30000, 500 * 2 ** (attempt - 1));
      await sleep(backoff);
      continue;
    }

    const body = typeof res.data === "string" ? res.data : JSON.stringify(res.data);
    throw new Error(`HTTP ${res.status}: ${body}`);
  }

  throw new Error(`Request failed after ${maxAttempts} attempts`);
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
async function loadSkuInventoryMap() {
  // We expect inventory item id to exist in attributes from product sync.
  const q = `
    select
      sku,
      coalesce(
        nullif(attributes->>'inventory_item_id',''),
        nullif(attributes->>'shopify_inventory_item_id','')
      ) as inventory_item_id,
      shopify_variant_id
    from ops.dim_sku
    where sku is not null
  `;
  const res = await pool.query(q);

  const invIdToSku = new Map();
  let withInvId = 0;

  for (const r of res.rows) {
    const inv = r.inventory_item_id ? Number(r.inventory_item_id) : null;
    if (Number.isFinite(inv) && inv > 0) {
      invIdToSku.set(inv, r.sku);
      withInvId++;
    }
  }

  return { invIdToSku, totalSkus: res.rowCount, skusWithInvId: withInvId };
}

async function upsertInventorySnapshotRow(client, { snapshotDate, sku, locationId, locationName, available, raw }) {
  const q = `
    insert into ops.fact_inventory_snapshot
      (snapshot_date, sku, shopify_variant_id, location_id, location_name, available, source, raw, ingested_at)
    values
      ($1::date, $2, null, $3, $4, $5, 'shopify', $6::jsonb, now())
    on conflict (snapshot_date, sku, location_id) do update set
      available = excluded.available,
      raw = excluded.raw,
      ingested_at = now()
  `;
  await client.query(q, [
    snapshotDate,
    sku,         
    locationId, 
    locationName, 
    available,    
    JSON.stringify(raw || {}), 
  ]);
}
async function fetchLocations() {
  const res = await requestWithRetry("GET", "/locations.json", { params: { limit: LIMIT } });
  const locations = res.data?.locations || [];
  if (LOCATION_NAME_ALLOWLIST.length) {
    return locations.filter((l) => LOCATION_NAME_ALLOWLIST.includes(l.name));
  }
  return locations;
}

async function fetchInventoryLevelsForLocation(locationId, inventoryItemIds) {

  let pageInfo = null;
  const out = [];

  while (true) {
    const params = pageInfo
      ? { limit: LIMIT, page_info: pageInfo }
      : {
          limit: LIMIT,
          location_ids: String(locationId),
          inventory_item_ids: inventoryItemIds.join(","),
        };

    const res = await requestWithRetry("GET", "/inventory_levels.json", { params });
    const levels = res.data?.inventory_levels || [];
    out.push(...levels);

    const next = extractNextPageInfo(res.headers?.link);
    pageInfo = next;
    if (!pageInfo) break;
  }

  return out;
}

async function main() {
  const snapshotDate = SNAPSHOT_DATE || todayUtcYYYYMMDD();

  const runMeta = {
    shop: SHOP,
    version: VERSION,
    rps: RPS,
    snapshot_date: snapshotDate,
    include_all_rollup: INCLUDE_ALL_ROLLUP,
    location_allowlist: LOCATION_NAME_ALLOWLIST,
    started_at: nowIso(),
  };

  const runId = await startIngestionRun(runMeta);

  console.log(`[${PIPELINE}] start | snapshot_date=${snapshotDate} | rps=${RPS}`);

  let rowsUpserted = 0;

  try {
    const { invIdToSku, totalSkus, skusWithInvId } = await loadSkuInventoryMap();
    if (!skusWithInvId) {
      throw new Error(
        `No inventory_item_id found in ops.dim_sku.attributes. Update product sync to store inventory_item_id first. totalSkus=${totalSkus}`
      );
    }

    const locations = await fetchLocations();
    if (!locations.length) {
      throw new Error("No Shopify locations returned. Check token permissions or store setup.");
    }

    const allInvIds = Array.from(invIdToSku.keys());
    const invChunks = chunk(allInvIds, INVENTORY_ITEM_ID_CHUNK);

    const client = await pool.connect();
    try {
      for (const loc of locations) {
        const locationId = Number(loc.id); 
        const locationName = loc.name || `location_${loc.id}`;

        for (const ids of invChunks) {
          const levels = await fetchInventoryLevelsForLocation(locationId, ids);

          // Write each returned level
          for (const lvl of levels) {
            const invId = Number(lvl.inventory_item_id);
            const sku = invIdToSku.get(invId);
            if (!sku) continue;

            const available = Number(lvl.available);
            await upsertInventorySnapshotRow(client, {
              snapshotDate,
              sku,
              locationId,
              locationName,
              available: Number.isFinite(available) ? available : 0,
              raw: lvl,
            });

            rowsUpserted++;
          }
        }
      }
    } finally {
      client.release();
    }
    await finishIngestionRun(runId, "succeeded", rowsUpserted, 0, null, {
      finished_at: nowIso(),
      total_skus: totalSkus,
      skus_with_inventory_item_id: skusWithInvId,
      locations: locations.map((l) => ({ id: l.id, name: l.name })),
    });

    console.log(`[${PIPELINE}] ok | rows_upserted=${rowsUpserted} | locations=${locations.length}`);
  } catch (err) {
    const msg = err.response?.data ? JSON.stringify(err.response.data) : err.message;
    await finishIngestionRun(runId, "failed", rowsUpserted, 0, msg, { finished_at: nowIso() });
    console.error(`[${PIPELINE}] failed | ${msg}`);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
}
main();