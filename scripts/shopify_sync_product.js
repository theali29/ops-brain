const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, "../.env") });

const axios = require("axios");
const { pool } = require("../config/db");

const SHOP = process.env.SHOPIFY_STORE; 
const TOKEN = process.env.SHOPIFY_ADMIN_TOKEN;
const VERSION = process.env.SHOPIFY_API_VERSION || "2024-10";

if (!SHOP || !TOKEN) {
  console.error("Missing SHOPIFY_STORE or SHOPIFY_ADMIN_TOKEN in .env");
  process.exit(1);
}

const api = axios.create({
  baseURL: `https://${SHOP}/admin/api/${VERSION}`,
  headers: {
    "X-Shopify-Access-Token": TOKEN,
    "Content-Type": "application/json",
  },
  timeout: 30000,
});

async function fetchAllProducts() {
  const all = [];
  let since_id = 0;

  while (true) {
    const res = await api.get("/products.json", {
      params: {
        limit: 250,
        status: "active",
        since_id,
        fields: "id,title,variants",
      },
    });

    const products = res.data.products || [];
    if (products.length === 0) break;

    all.push(...products);
    since_id = products[products.length - 1].id;

    if (products.length < 250) break;
  }

  return all;
}

async function upsertSku({
  sku,
  product_title,
  variant_title,
  shopify_product_id,
  shopify_variant_id,
  attributes,
}) {
  const q = `
    insert into ops.dim_sku
      (sku, product_title, variant_title, shopify_product_id, shopify_variant_id, attributes, updated_at)
    values
      ($1,  $2,           $3,            $4,               $5,                $6::jsonb, now())
    on conflict (sku) do update set
      product_title = excluded.product_title,
      variant_title = excluded.variant_title,
      shopify_product_id = excluded.shopify_product_id,
      shopify_variant_id = excluded.shopify_variant_id,
      attributes = excluded.attributes,
      updated_at = now()
  `;
  await pool.query(q, [
    sku,
    product_title,
    variant_title,
    shopify_product_id,
    shopify_variant_id,
    JSON.stringify(attributes || {}),
  ]);
}

async function main() {
//Syncing products from Le Jardin Switzerland to ops.dim_sku
  const products = await fetchAllProducts();
  let upserts = 0;
  for (const p of products) {
    const productTitle = p.title;

    for (const v of p.variants || []) {
      const sku = (v.sku || "").trim();
      if (!sku) continue; // skip variants without SKU

      await upsertSku({
        sku,
        product_title: productTitle,
        variant_title: v.title,
        shopify_product_id: p.id,
        shopify_variant_id: v.id,
        attributes: {
          inventory_item_id: v.inventory_item_id ?? null,
          inventory_management: v.inventory_management ?? null,
          inventory_policy: v.inventory_policy ?? null,
          fulfillment_service: v.fulfillment_service ?? null,
          price: v.price ?? null,
          compare_at_price: v.compare_at_price ?? null,
          requires_shipping: v.requires_shipping ?? null,
          taxable: v.taxable ?? null,
          barcode: v.barcode ?? null,
          weight: v.weight ?? null,
          weight_unit: v.weight_unit ?? null,
        },
      });

      upserts++;
    }
  }

  await pool.end();
}

main().catch(async (err) => {
  console.error("ERROR:", err.response?.data || err.message);
  try {
    await pool.end();
  } catch {}
  process.exit(1);
});
