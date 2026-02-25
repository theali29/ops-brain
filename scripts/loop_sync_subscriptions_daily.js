"use strict";

const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, "../.env") });

const { withIngestionRun } = require("./lib/ingestionRun");
const { createLoopClient, epochToIso } = require("./lib/loop");

const PIPELINE = "loop_subscriptions_daily";
const SOURCE = "loop";
const STATE_KEY = "loop_subscriptions_daily";

const PAGE_SIZE = Math.min(Number(process.env.LOOP_PAGE_SIZE || 50), 50);
const DEFAULT_LOOKBACK_DAYS = Number(process.env.DEFAULT_LOOKBACK_DAYS || 7);
const OVERLAP_HOURS = Number(process.env.OVERLAP_HOURS || 48);

const loop = createLoopClient();

function nowIso() {
  return new Date().toISOString();
}

function isoDaysAgo(days) {
  return new Date(Date.now() - days * 86400 * 1000).toISOString();
}

function isoHoursAgoFrom(iso, hours) {
  const base = new Date(iso);
  return new Date(base.getTime() - hours * 3600 * 1000).toISOString();
}

function toEpochSec(iso) {
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : null;
}

function toNumOrNull(x) {
  if (x === null || x === undefined) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

async function getSyncCursor(client, key) {
  const res = await client.query("select value from ops.sync_state where key = $1", [key]);
  if (!res.rowCount) return null;
  return res.rows[0].value;
}

async function setSyncCursor(client, key, value) {
  const q = `
    insert into ops.sync_state (key, value, updated_at)
    values ($1, $2::jsonb, now())
    on conflict (key) do update set
      value = excluded.value,
      updated_at = now()
  `;
  await client.query(q, [key, JSON.stringify(value)]);
}

// DB upserts
async function upsertSubscriptionHeader(client, sub) {
  const nextBillingAtIso = epochToIso(sub.nextBillingDateEpoch);

  const shipping = sub.shippingAddress || {};
  const billingPolicy = sub.billingPolicy || {};
  const deliveryPolicy = sub.deliveryPolicy || {};
  const deliveryMethod = sub.deliveryMethod || {};
  const customer = sub.customer || {};

  const q = `
    insert into ops.fact_loop_subscriptions
      (subscription_id, shopify_subscription_id, origin_order_shopify_id,
       loop_customer_id, shopify_customer_id,
       status, created_at_loop, updated_at_loop,
       paused_at, cancelled_at,
       cancellation_reason, cancellation_comment,
       completed_orders_count,
       is_prepaid, is_marked_for_cancellation,
       last_payment_status, currency_code,
       total_line_item_price, total_line_item_discounted_price, delivery_price,
       next_billing_date_epoch, next_billing_at,
       billing_interval, billing_interval_count,
       delivery_interval, delivery_interval_count,
       delivery_method_code, delivery_method_title,
       shipping_city, shipping_zip, shipping_country_code, shipping_province_code,
       raw, ingested_at)
    values
      ($1,$2,$3,
       $4,$5,
       $6,$7,$8,
       $9,$10,
       $11,$12,
       $13,
       $14,$15,
       $16,$17,
       $18,$19,$20,
       $21,$22,
       $23,$24,
       $25,$26,
       $27,$28,
       $29,$30,$31,$32,
       $33::jsonb, now())
    on conflict (subscription_id) do update set
       shopify_subscription_id = excluded.shopify_subscription_id,
       origin_order_shopify_id = excluded.origin_order_shopify_id,
       loop_customer_id = excluded.loop_customer_id,
       shopify_customer_id = excluded.shopify_customer_id,
       status = excluded.status,
       created_at_loop = excluded.created_at_loop,
       updated_at_loop = excluded.updated_at_loop,
       paused_at = excluded.paused_at,
       cancelled_at = excluded.cancelled_at,
       cancellation_reason = excluded.cancellation_reason,
       cancellation_comment = excluded.cancellation_comment,
       completed_orders_count = excluded.completed_orders_count,
       is_prepaid = excluded.is_prepaid,
       is_marked_for_cancellation = excluded.is_marked_for_cancellation,
       last_payment_status = excluded.last_payment_status,
       currency_code = excluded.currency_code,
       total_line_item_price = excluded.total_line_item_price,
       total_line_item_discounted_price = excluded.total_line_item_discounted_price,
       delivery_price = excluded.delivery_price,
       next_billing_date_epoch = excluded.next_billing_date_epoch,
       next_billing_at = excluded.next_billing_at,
       billing_interval = excluded.billing_interval,
       billing_interval_count = excluded.billing_interval_count,
       delivery_interval = excluded.delivery_interval,
       delivery_interval_count = excluded.delivery_interval_count,
       delivery_method_code = excluded.delivery_method_code,
       delivery_method_title = excluded.delivery_method_title,
       shipping_city = excluded.shipping_city,
       shipping_zip = excluded.shipping_zip,
       shipping_country_code = excluded.shipping_country_code,
       shipping_province_code = excluded.shipping_province_code,
       raw = excluded.raw,
       ingested_at = now()
  `;

  await client.query(q, [
    sub.id ?? null,
    sub.shopifyId ?? null,
    sub.originOrderShopifyId ?? null,

    customer.id ?? null,
    customer.shopifyId ?? null,

    sub.status ?? null,
    sub.createdAt ?? null,
    sub.updatedAt ?? null,

    sub.pausedAt ?? null,
    sub.cancelledAt ?? null,

    sub.cancellationReason ?? null,
    sub.cancellationComment ?? null,

    sub.completedOrdersCount ?? null,

    sub.isPrepaid ?? null,
    sub.isMarkedForCancellation ?? null,

    sub.lastPaymentStatus ?? null,
    sub.currencyCode ?? null,

    toNumOrNull(sub.totalLineItemPrice),
    toNumOrNull(sub.totalLineItemDiscountedPrice),
    toNumOrNull(sub.deliveryPrice),

    sub.nextBillingDateEpoch ?? null,
    nextBillingAtIso,

    billingPolicy.interval ?? null,
    billingPolicy.intervalCount ?? null,

    deliveryPolicy.interval ?? null,
    deliveryPolicy.intervalCount ?? null,

    deliveryMethod.code ?? null,
    deliveryMethod.title ?? null,

    shipping.city ?? null,
    shipping.zip ?? null,
    shipping.countryCode ?? null,
    shipping.provinceCode ?? null,

    JSON.stringify(sub),
  ]);
}

async function ensureSkuExistsBestEffort(client, sku, attrs) {
  const s = (sku || "").trim();
  if (!s) return false;

  const check = await client.query("select 1 from ops.dim_sku where sku = $1 limit 1", [s]);
  if (check.rowCount) return false;

  const q = `
    insert into ops.dim_sku
      (sku, product_title, variant_title, shopify_product_id, shopify_variant_id, attributes, created_at, updated_at)
    values
      ($1,$2,$3,$4,$5,$6::jsonb, now(), now())
    on conflict (sku) do nothing
  `;

  await client.query(q, [
    s,
    attrs?.product_title ?? null,
    attrs?.variant_title ?? null,
    attrs?.shopify_product_id ?? null,
    attrs?.shopify_variant_id ?? null,
    JSON.stringify({
      stub: true,
      stub_reason: "seen_in_loop_subscription_line",
      created_by: PIPELINE,
      ...attrs,
    }),
  ]);

  return true;
}

async function upsertSubscriptionLine(client, subscriptionId, line) {
  let stubInserted = false;

  if (line?.sku) {
    stubInserted = await ensureSkuExistsBestEffort(client, line.sku, {
      product_title: line.productTitle ?? null,
      variant_title: line.variantTitle ?? null,
      shopify_product_id: line.productShopifyId ?? null,
      shopify_variant_id: line.variantShopifyId ?? null,
    });
  }

  const q = `
    insert into ops.fact_loop_subscription_lines
      (subscription_id, line_id,
       sku, quantity,
       product_shopify_id, variant_shopify_id,
       selling_plan_shopify_id, selling_plan_name,
       selling_plan_group_name, selling_plan_group_merchant_code,
       product_title, variant_title, line_name,
       price, base_price, discounted_price,
       is_one_time_added, is_one_time_removed,
       weight_in_grams,
       raw, ingested_at)
    values
      ($1,$2,
       $3,$4,
       $5,$6,
       $7,$8,
       $9,$10,
       $11,$12,$13,
       $14,$15,$16,
       $17,$18,
       $19,
       $20::jsonb, now())
    on conflict (subscription_id, line_id) do update set
       sku = excluded.sku,
       quantity = excluded.quantity,
       product_shopify_id = excluded.product_shopify_id,
       variant_shopify_id = excluded.variant_shopify_id,
       selling_plan_shopify_id = excluded.selling_plan_shopify_id,
       selling_plan_name = excluded.selling_plan_name,
       selling_plan_group_name = excluded.selling_plan_group_name,
       selling_plan_group_merchant_code = excluded.selling_plan_group_merchant_code,
       product_title = excluded.product_title,
       variant_title = excluded.variant_title,
       line_name = excluded.line_name,
       price = excluded.price,
       base_price = excluded.base_price,
       discounted_price = excluded.discounted_price,
       is_one_time_added = excluded.is_one_time_added,
       is_one_time_removed = excluded.is_one_time_removed,
       weight_in_grams = excluded.weight_in_grams,
       raw = excluded.raw,
       ingested_at = now()
  `;

  await client.query(q, [
    subscriptionId,
    line.id ?? null,

    (line.sku || "").trim() || null,
    line.quantity ?? 0,

    line.productShopifyId ?? null,
    line.variantShopifyId ?? null,

    line.sellingPlanShopifyId ?? null,
    line.sellingPlanName ?? null,

    line.sellingPlanGroupName ?? null,
    line.sellingPlanGroupMerchantCode ?? null,

    line.productTitle ?? null,
    line.variantTitle ?? null,
    line.name ?? null,

    toNumOrNull(line.price),
    toNumOrNull(line.basePrice),
    toNumOrNull(line.discountedPrice),

    line.isOneTimeAdded ?? null,
    line.isOneTimeRemoved ?? null,

    line.weightInGrams ?? null,

    JSON.stringify(line),
  ]);

  return { stubInserted };
}

async function main() {
  await withIngestionRun(
    {
      source: SOURCE,
      pipeline: PIPELINE,
      metadata: {
        endpoint: "/subscription",
        page_size: PAGE_SIZE,
        overlap_hours: OVERLAP_HOURS,
        started_at: nowIso(),
      },
    },
    async (ctx) => {
      let stubSkusInserted = 0;
      let maxUpdatedAtSeen = null;

      const client = await ctx.pool.connect();
      ctx.setClient(client);

      try {
        const cursor = await getSyncCursor(client, STATE_KEY);

        // Canonical cursor: last_end_iso
        // if old state exists, treat last_max_updated_at as last_end_iso
        const lastEndIso =
          cursor?.last_end_iso ||
          cursor?.last_max_updated_at ||
          isoDaysAgo(DEFAULT_LOOKBACK_DAYS);

        const windowStartIso =
          OVERLAP_HOURS > 0 ? isoHoursAgoFrom(lastEndIso, OVERLAP_HOURS) : lastEndIso;
        const windowEndIso = nowIso();

        const startEpoch = toEpochSec(windowStartIso);
        const endEpoch = toEpochSec(windowEndIso);

        if (startEpoch === null || endEpoch === null) {
          throw new Error(`Invalid window ISO: start=${windowStartIso} end=${windowEndIso}`);
        }

        console.log(
          `[${PIPELINE}] start | window=${windowStartIso} -> ${windowEndIso} startEpoch=${startEpoch} endEpoch=${endEpoch} page_size=${PAGE_SIZE}`
        );

        let pageNo = 1;

        while (true) {
          const { data, pageInfo } = await loop.getPaged("/subscription", {
            pageNo,
            pageSize: PAGE_SIZE,
            updatedAtStartEpoch: startEpoch,
            updatedAtEndEpoch: endEpoch,
          });

          if (!Array.isArray(data) || data.length === 0) break;

          await client.query("BEGIN");
          ctx.beginTxn();

          try {
            for (const sub of data) {
              if (sub?.updatedAt) {
                if (!maxUpdatedAtSeen || Date.parse(sub.updatedAt) > Date.parse(maxUpdatedAtSeen)) {
                  maxUpdatedAtSeen = sub.updatedAt;
                }
              }

              await upsertSubscriptionHeader(client, sub);
              ctx.incrementUpserts(1);

              const lines = Array.isArray(sub.lines) ? sub.lines : [];
              for (const line of lines) {
                const { stubInserted } = await upsertSubscriptionLine(client, sub.id, line);
                ctx.incrementUpserts(1);
                if (stubInserted) stubSkusInserted++;
              }
            }

            await client.query("COMMIT");
            ctx.endTxn();
          } catch (e) {
            await client.query("ROLLBACK");
            ctx.endTxn();
            throw e;
          }

          console.log(
            `[${PIPELINE}] page ${pageNo} done | returned=${data.length} | hasNextPage=${!!pageInfo?.hasNextPage}`
          );

          if (!pageInfo?.hasNextPage) break;
          pageNo += 1;
        }

        await setSyncCursor(client, STATE_KEY, {
          last_end_iso: windowEndIso,
          last_max_updated_at_seen: maxUpdatedAtSeen,
        });

        console.log(
          `[${PIPELINE}] success | stub_skus_inserted=${stubSkusInserted} new_last_end_iso=${windowEndIso} maxUpdatedAtSeen=${maxUpdatedAtSeen || "null"}`
        );
      } finally {
        client.release();
      }
    }
  );
}

main();