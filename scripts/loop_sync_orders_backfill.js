"use strict";

const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, "../.env") });

const { withIngestionRun } = require("./lib/ingestionRun"); 
const { createLoopClient, epochToIso } = require("./lib/loop");

const PIPELINE = "loop_orders_backfill";
const SOURCE = "loop";

const PAGE_SIZE = Math.min(Number(process.env.LOOP_PAGE_SIZE || 50), 50);
const LOOP_ORDERS_PATH = "/order";

const loop = createLoopClient();

function toNumOrNull(x) {
  if (x === null || x === undefined) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
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
      stub_reason: "seen_in_loop_order_line",
      created_by: PIPELINE,
      ...attrs,
    }),
  ]);

  return true;
}

async function upsertOrderHeader(client, order) {
  const billingAtIso = epochToIso(order.billingDateEpoch);

  const shipping = order.shippingAddress || {};
  const customer = order.customer || {};

  const q = `
    insert into ops.fact_loop_orders
      (loop_order_id, shopify_order_id, shopify_order_number,
       status, fulfillment_status, financial_status,
       billing_date_epoch, billing_at,
       shopify_created_at, shopify_processed_at, shopify_updated_at,
       currency_code,
       total_price, total_price_usd, total_tax, total_discount, total_line_items_price, total_shipping_price,
       is_checkout_order, order_type,
       updated_at_loop,
       loop_customer_id, shopify_customer_id,
       subscription_id,
       shipping_city, shipping_zip, shipping_country_code, shipping_province_code,
       raw, ingested_at)
    values
      ($1,$2,$3,
       $4,$5,$6,
       $7,$8,
       $9,$10,$11,
       $12,
       $13,$14,$15,$16,$17,$18,
       $19,$20,
       $21,
       $22,$23,
       $24,
       $25,$26,$27,$28,
       $29::jsonb, now())
    on conflict (loop_order_id) do update set
       shopify_order_id = excluded.shopify_order_id,
       shopify_order_number = excluded.shopify_order_number,
       status = excluded.status,
       fulfillment_status = excluded.fulfillment_status,
       financial_status = excluded.financial_status,
       billing_date_epoch = excluded.billing_date_epoch,
       billing_at = excluded.billing_at,
       shopify_created_at = excluded.shopify_created_at,
       shopify_processed_at = excluded.shopify_processed_at,
       shopify_updated_at = excluded.shopify_updated_at,
       currency_code = excluded.currency_code,
       total_price = excluded.total_price,
       total_price_usd = excluded.total_price_usd,
       total_tax = excluded.total_tax,
       total_discount = excluded.total_discount,
       total_line_items_price = excluded.total_line_items_price,
       total_shipping_price = excluded.total_shipping_price,
       is_checkout_order = excluded.is_checkout_order,
       order_type = excluded.order_type,
       updated_at_loop = excluded.updated_at_loop,
       loop_customer_id = excluded.loop_customer_id,
       shopify_customer_id = excluded.shopify_customer_id,
       subscription_id = excluded.subscription_id,
       shipping_city = excluded.shipping_city,
       shipping_zip = excluded.shipping_zip,
       shipping_country_code = excluded.shipping_country_code,
       shipping_province_code = excluded.shipping_province_code,
       raw = excluded.raw,
       ingested_at = now()
  `;

  await client.query(q, [
    order.id ?? null,
    order.shopifyId ?? null,
    order.shopifyOrderNumber ?? null,

    order.status ?? null,
    order.fulfillmentStatus ?? null,
    order.financialStatus ?? null,

    order.billingDateEpoch ?? null,
    billingAtIso,

    order.shopifyCreatedAt ?? null,
    order.shopifyProcessedAt ?? null,
    order.shopifyUpdatedAt ?? null,

    order.currencyCode ?? null,

    toNumOrNull(order.totalPrice),
    toNumOrNull(order.totalPriceUsd),
    toNumOrNull(order.totalTax),
    toNumOrNull(order.totalDiscount),
    toNumOrNull(order.totalLineItemsPrice),
    toNumOrNull(order.totalShippingPrice),

    order.isCheckoutOrder ?? null,
    order.orderType ?? null,

    order.updatedAt ?? null,

    customer.id ?? null,
    customer.shopifyId ?? null,

    order.subscriptionId ?? null,

    shipping.city ?? null,
    shipping.zip ?? null,
    shipping.countryCode ?? null,
    shipping.provinceCode ?? null,

    JSON.stringify(order),
  ]);
}

async function upsertOrderLine(client, loopOrderId, line) {
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
    insert into ops.fact_loop_order_lines
      (loop_order_id,
       sku, quantity, price,
       product_shopify_id, variant_shopify_id,
       product_title, variant_title, line_name,
       is_one_time, current_quantity,
       raw, ingested_at)
    values
      ($1,
       $2,$3,$4,
       $5,$6,
       $7,$8,$9,
       $10,$11,
       $12::jsonb, now())
    on conflict (loop_order_id, coalesce(variant_shopify_id, 0), coalesce(sku, ''), is_one_time)
    do update set
       quantity = excluded.quantity,
       price = excluded.price,
       product_shopify_id = excluded.product_shopify_id,
       variant_shopify_id = excluded.variant_shopify_id,
       product_title = excluded.product_title,
       variant_title = excluded.variant_title,
       line_name = excluded.line_name,
       current_quantity = excluded.current_quantity,
       raw = excluded.raw,
       ingested_at = now()
  `;

  await client.query(q, [
    loopOrderId,

    (line.sku || "").trim() || null,
    line.quantity ?? 0,
    toNumOrNull(line.price),

    line.productShopifyId ?? null,
    line.variantShopifyId ?? null,

    line.productTitle ?? null,
    line.variantTitle ?? null,
    line.name ?? null,

    line.isOneTime ?? false,
    line.currentQuantity ?? null,

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
        endpoint: LOOP_ORDERS_PATH,
        page_size: PAGE_SIZE,
        started_at: new Date().toISOString(),
      },
    },
    async (ctx) => {
      let stubSkusInserted = 0;

      const client = await ctx.pool.connect();
      ctx.setClient(client);

      try {
        let pageNo = 1;

        while (true) {
          const { data, pageInfo } = await loop.getPaged(LOOP_ORDERS_PATH, {
            pageNo,
            pageSize: PAGE_SIZE,
          });

          if (!Array.isArray(data) || data.length === 0) break;

          await client.query("BEGIN");
          ctx.beginTxn();

          try {
            for (const order of data) {
              await upsertOrderHeader(client, order);
              ctx.incrementUpserts(1);

              const lines = Array.isArray(order.lines) ? order.lines : [];
              for (const line of lines) {
                const { stubInserted } = await upsertOrderLine(client, order.id, line);
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
            `[${PIPELINE}] page ${pageNo} done | orders=${data.length} | hasNextPage=${!!pageInfo?.hasNextPage}`
          );

          if (!pageInfo?.hasNextPage) break;
          pageNo += 1;
        }

        console.log(`[${PIPELINE}] stub_skus_inserted=${stubSkusInserted}`);
      } finally {
        client.release();
      }
    }
  );
}

main();