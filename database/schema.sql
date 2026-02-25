create schema if not exists ops;

-- ---- Enums for cleanliness / safety ----
do $$ begin
  create type ops.data_source as enum ('shopify','google_sheets','loop','manual');
exception when duplicate_object then null;
end $$;

do $$ begin
  create type ops.sync_status as enum ('started','succeeded','failed');
exception when duplicate_object then null;
end $$;

-- ---- Ingestion audit (freshness + traceability) ----
create table if not exists ops.ingestion_runs (
  id              uuid primary key default gen_random_uuid(),
  source          ops.data_source not null,
  pipeline        text not null, -- e.g. 'shopify_orders', 'shopify_inventory', 'sheets_inbound'
  started_at      timestamptz not null default now(),
  finished_at     timestamptz,
  status          ops.sync_status not null default 'started',
  rows_upserted   bigint not null default 0,
  rows_deleted    bigint not null default 0,
  error_message   text,
  metadata        jsonb not null default '{}'::jsonb
);

create index if not exists ingestion_runs_idx
  on ops.ingestion_runs (source, pipeline, started_at desc);

-- ---- Dimension: SKU (the join spine for everything) ----
create table if not exists ops.dim_sku (
  sku                 text primary key,
  product_title        text,
  variant_title        text,
  shopify_product_id   bigint,
  shopify_variant_id   bigint,
  is_bundle            boolean not null default false,
  is_subscription      boolean not null default false,
  is_active            boolean not null default true,
  attributes           jsonb not null default '{}'::jsonb,
  created_at           timestamptz not null default now(),
  updated_at           timestamptz not null default now()
);

create index if not exists dim_sku_variant_idx
  on ops.dim_sku (shopify_variant_id);

-- Shopify (Revenue truth)

-- Order header: 1 row per order
create table if not exists ops.fact_shopify_orders (
  order_id            bigint primary key,            -- Shopify order id
  order_number        text,
  customer_id         bigint,
  processed_at        timestamptz not null,
  created_at_shopify  timestamptz,
  currency            text not null,
  financial_status    text,
  fulfillment_status  text,
  cancelled_at        timestamptz,
  cancel_reason       text,
  tags                text[],
  is_test             boolean not null default false,
  source              ops.data_source not null default 'shopify',
  raw                 jsonb not null default '{}'::jsonb, -- for debugging
  ingested_at         timestamptz not null default now()
);

create index if not exists shopify_orders_processed_idx
  on ops.fact_shopify_orders (processed_at desc);

-- Line items: SKU-level units and revenue attribution
create table if not exists ops.fact_shopify_order_line_items (
  id                         bigserial primary key,
  order_id                   bigint not null references ops.fact_shopify_orders(order_id) on delete cascade,
  line_item_id               bigint not null,               -- unique within order
  sku                        text not null references ops.dim_sku(sku),
  shopify_product_id         bigint,
  shopify_variant_id         bigint,
  shopify_inventory_item_id  bigint, 
  title                      text,
  variant_title              text,
  quantity                   integer not null check (quantity <> 0),
  fulfillment_service        text,
  gross_item_revenue         numeric(18,2) not null default 0, -- pre-discount
  discount_amount            numeric(18,2) not null default 0,
  tax_amount                 numeric(18,2) not null default 0,
  net_item_revenue           numeric(18,2) not null default 0, -- gross - discount (refunds separate)

  is_subscription            boolean not null default false,
  source                     ops.data_source not null default 'shopify',
  ingested_at                timestamptz not null default now(),

  unique (order_id, line_item_id)
);

create index if not exists line_items_sku_idx
  on ops.fact_shopify_order_line_items (sku);

create index if not exists line_items_variant_idx
  on ops.fact_shopify_order_line_items (shopify_variant_id);

-- Refund headers
create table if not exists ops.fact_shopify_refunds (
  refund_id           bigint primary key,
  order_id            bigint not null references ops.fact_shopify_orders(order_id) on delete cascade,
  created_at_refund   timestamptz not null,
  note                text,
  source              ops.data_source not null default 'shopify',
  raw                 jsonb not null default '{}'::jsonb,
  ingested_at         timestamptz not null default now()
);

create index if not exists refunds_order_idx
  on ops.fact_shopify_refunds (order_id);

-- Refund line items (SKU-level refund attribution)
create table if not exists ops.fact_shopify_refund_line_items (
  id                 bigserial primary key,
  refund_id           bigint not null references ops.fact_shopify_refunds(refund_id) on delete cascade,
  order_id            bigint not null references ops.fact_shopify_orders(order_id) on delete cascade,
  line_item_id        bigint,
  sku                 text references ops.dim_sku(sku),
  quantity            integer not null check (quantity >= 0),
  refund_amount       numeric(18,2) not null default 0,
  source              ops.data_source not null default 'shopify',
  ingested_at         timestamptz not null default now()
);

create index if not exists refund_line_items_sku_idx
  on ops.fact_shopify_refund_line_items (sku);

-- Shopify Inventory (Operational truth for MVP)
create table if not exists ops.fact_inventory_snapshot (
  id               bigserial primary key,
  snapshot_date    date not null,
  sku              text not null references ops.dim_sku(sku),
  shopify_variant_id bigint,
  location_name    text not null default 'all', -- 'US Warehouse', 'ShipRelay', etc or 'all'
  available        integer not null,            -- can be negative (backorders)
  source           ops.data_source not null default 'shopify',
  raw              jsonb not null default '{}'::jsonb, -- helpful for debugging location mapping
  ingested_at      timestamptz not null default now(),
  unique (snapshot_date, sku, location_name)
);

create index if not exists inv_snapshot_sku_date_idx
  on ops.fact_inventory_snapshot (sku, snapshot_date desc);

create or replace view ops.v_latest_inventory as
select distinct on (sku, location_name)
  sku, location_name, snapshot_date,
  available,
  ingested_at
from ops.fact_inventory_snapshot
order by sku, location_name, snapshot_date desc, ingested_at desc;

-- Sheets (Planning brain)

-- Inbound supply / production arrivals
create table if not exists ops.fact_inbound_shipments (
  id              uuid primary key default gen_random_uuid(),
  sku             text not null references ops.dim_sku(sku),
  batch_name       text,
  arrival_month    date not null, -- normalize to YYYY-MM-01
  arrival_date     date,          -- optional ETA day if available
  quantity         integer not null check (quantity >= 0),
  location_name    text,
  source           ops.data_source not null default 'google_sheets',
  sheet_row_key    text not null, -- stable for upsert, e.g. '2026-03|THM|B007'
  ingested_at      timestamptz not null default now(),
  unique (source, sheet_row_key)
);

create index if not exists inbound_sku_month_idx
  on ops.fact_inbound_shipments (sku, arrival_month);

-- Monthly demand forecast (from the inventory/cashflow sheet)
create table if not exists ops.fact_demand_forecast (
  id                         uuid primary key default gen_random_uuid(),
  sku                        text not null references ops.dim_sku(sku),
  month                      date not null, -- YYYY-MM-01
  forecast_total_units        integer not null check (forecast_total_units >= 0),
  forecast_new_customer_units integer not null default 0 check (forecast_new_customer_units >= 0),
  forecast_subscription_units integer not null default 0 check (forecast_subscription_units >= 0),
  model_version              text not null default 'sheet_v1',
  source                     ops.data_source not null default 'google_sheets',
  sheet_row_key              text,
  ingested_at                timestamptz not null default now(),
  unique (source, sku, month, model_version)
);

create index if not exists demand_forecast_sku_month_idx
  on ops.fact_demand_forecast (sku, month);

-- Reorder policy 
create table if not exists ops.dim_reorder_rules (
  sku                      text primary key references ops.dim_sku(sku),
  lead_time_months          numeric(6,2) not null check (lead_time_months >= 0),
  safety_buffer_months      numeric(6,2) not null check (safety_buffer_months >= 0),
  reorder_trigger_months    numeric(6,2) not null check (reorder_trigger_months >= 0),
  standard_order_qty        integer not null check (standard_order_qty >= 0),
  min_runway_trigger_months numeric(6,2),
  source                   ops.data_source not null default 'google_sheets',
  ingested_at              timestamptz not null default now()
);

-- Unit economics / SKU financials
create table if not exists ops.dim_sku_financials (
  sku                        text primary key references ops.dim_sku(sku),
  unit_sell_price            numeric(18,2),
  unit_cogs                  numeric(18,4),
  shipping_cost_per_order    numeric(18,4),
  fulfillment_cost_per_unit  numeric(18,4),
  payment_fee_rate           numeric(8,6),
  return_rate                numeric(8,6),
  notes                      text,
  source                     ops.data_source not null default 'google_sheets',
  ingested_at                timestamptz not null default now()
);
create table if not exists ops.sync_state (
  key text primary key,
  value jsonb not null,
  updated_at timestamptz not null default now()
);

create unique index if not exists refund_line_items_uniq
  on ops.fact_shopify_refund_line_items (refund_id, line_item_id)
  where line_item_id is not null;

-- Subscription header (1 row per Loop subscription)
create table if not exists ops.fact_loop_subscriptions (
  subscription_id            bigint primary key,                 -- Loop internal id
  shopify_subscription_id    bigint,                             -- Loop field: shopifyId
  origin_order_shopify_id    bigint,                             -- Loop field: originOrderShopifyId

  loop_customer_id           bigint,                             -- Loop customer.id
  shopify_customer_id        bigint,                             -- Loop customer.shopifyId

  status                     text not null,                      -- ACTIVE | PAUSED | CANCELLED | EXPIRED
  created_at_loop            timestamptz,
  updated_at_loop            timestamptz,

  paused_at                  timestamptz,
  cancelled_at               timestamptz,

  cancellation_reason        text,
  cancellation_comment       text,

  completed_orders_count     integer,

  is_prepaid                 boolean,
  is_marked_for_cancellation boolean,

  last_payment_status        text,                               -- SUCCESS / FAILED etc
  currency_code              text,

  -- pricing rollups on the contract (optional but useful)
  total_line_item_price              numeric(18,2),
  total_line_item_discounted_price   numeric(18,2),
  delivery_price                     numeric(18,2),

  -- renewal timing
  next_billing_date_epoch    bigint,                             -- as provided by Loop
  next_billing_at            timestamptz,                         -- derived at ingestion time for easy SQL

  -- cadence (from billingPolicy / deliveryPolicy)
  billing_interval           text,                               -- e.g. MONTH
  billing_interval_count     integer,
  delivery_interval          text,
  delivery_interval_count    integer,

  -- logistics
  delivery_method_code       text,
  delivery_method_title      text,

  shipping_city              text,
  shipping_zip               text,
  shipping_country_code      text,
  shipping_province_code     text,

  source                     ops.data_source not null default 'loop',
  raw                        jsonb not null default '{}'::jsonb,
  ingested_at                timestamptz not null default now()
);

create index if not exists loop_subscriptions_status_idx
  on ops.fact_loop_subscriptions (status);

create index if not exists loop_subscriptions_next_billing_idx
  on ops.fact_loop_subscriptions (next_billing_at);

create index if not exists loop_subscriptions_updated_idx
  on ops.fact_loop_subscriptions (updated_at_loop desc);

create index if not exists loop_subscriptions_shopify_customer_idx
  on ops.fact_loop_subscriptions (shopify_customer_id);


-- Subscription lines
create table if not exists ops.fact_loop_subscription_lines (
  id                        bigserial primary key,

  subscription_id           bigint not null references ops.fact_loop_subscriptions(subscription_id) on delete cascade,
  line_id                   bigint not null,                   

  sku                       text references ops.dim_sku(sku),    -- nullable if missing
  quantity                  integer not null check (quantity >= 0),

  product_shopify_id        bigint,
  variant_shopify_id        bigint,

  selling_plan_shopify_id   bigint,
  selling_plan_name         text,
  selling_plan_group_name   text,
  selling_plan_group_merchant_code text,

  product_title             text,
  variant_title             text,
  line_name                 text,

  price                     numeric(18,2),
  base_price                numeric(18,2),
  discounted_price          numeric(18,2),

  is_one_time_added         boolean,
  is_one_time_removed       boolean,

  weight_in_grams           integer,

  source                    ops.data_source not null default 'loop',
  raw                       jsonb not null default '{}'::jsonb,
  ingested_at               timestamptz not null default now(),

  unique (subscription_id, line_id)
);

create index if not exists loop_sub_lines_sku_idx
  on ops.fact_loop_subscription_lines (sku);

create index if not exists loop_sub_lines_variant_idx
  on ops.fact_loop_subscription_lines (variant_shopify_id);


-- C) Loop orders header (1 row per Loop order event)
create table if not exists ops.fact_loop_orders (
  loop_order_id            bigint primary key,                 -- Loop order id
  shopify_order_id         bigint,                             -- Loop field: shopifyId
  shopify_order_number     bigint,                           

  status                   text,                               -- PROCESSED, etc
  fulfillment_status       text,
  financial_status         text,

  billing_date_epoch       bigint,
  billing_at               timestamptz,

  shopify_created_at       timestamptz,
  shopify_processed_at     timestamptz,
  shopify_updated_at       timestamptz,

  currency_code            text,

  total_price              numeric(18,2),
  total_price_usd          numeric(18,2),
  total_tax                numeric(18,2),
  total_discount           numeric(18,2),
  total_line_items_price   numeric(18,2),
  total_shipping_price     numeric(18,2),

  is_checkout_order        boolean,
  order_type               text,  

  updated_at_loop          timestamptz,

  loop_customer_id         bigint,
  shopify_customer_id      bigint,

  subscription_id          bigint references ops.fact_loop_subscriptions(subscription_id),

  shipping_city            text,
  shipping_zip             text,
  shipping_country_code    text,
  shipping_province_code   text,

  source                   ops.data_source not null default 'loop',
  raw                      jsonb not null default '{}'::jsonb,
  ingested_at              timestamptz not null default now()
);

create index if not exists loop_orders_subscription_idx
  on ops.fact_loop_orders (subscription_id);

create index if not exists loop_orders_billing_at_idx
  on ops.fact_loop_orders (billing_at);

create index if not exists loop_orders_updated_idx
  on ops.fact_loop_orders (updated_at_loop desc);

create index if not exists loop_orders_shopify_order_idx
  on ops.fact_loop_orders (shopify_order_id);


-- D) Loop order lines (SKU-level order items)
create table if not exists ops.fact_loop_order_lines (
  id                       bigserial primary key,

  loop_order_id            bigint not null references ops.fact_loop_orders(loop_order_id) on delete cascade,

  sku                      text references ops.dim_sku(sku),
  quantity                 integer not null check (quantity >= 0),
  price                    numeric(18,2),

  product_shopify_id       bigint,
  variant_shopify_id       bigint,

  product_title            text,
  variant_title            text,
  line_name                text,

  is_one_time              boolean not null default false,
  current_quantity         integer,

  source                   ops.data_source not null default 'loop',
  raw                      jsonb not null default '{}'::jsonb,
  ingested_at              timestamptz not null default now()
);

create unique index if not exists loop_order_lines_uniq
  on ops.fact_loop_order_lines (loop_order_id, coalesce(variant_shopify_id, 0), coalesce(sku, ''), is_one_time);

create index if not exists loop_order_lines_sku_idx
  on ops.fact_loop_order_lines (sku);

create index if not exists loop_order_lines_variant_idx
  on ops.fact_loop_order_lines (variant_shopify_id);


  -- Raw snapshot for audit/debug 
create table if not exists ops.sheet_tab_snapshots (
  id uuid primary key default gen_random_uuid(),
  sheet_key text not null,
  tab_name text not null,
  captured_at timestamptz not null default now(),
  range_a1 text not null,
  values jsonb not null
);

create index if not exists sheet_tab_snapshots_idx
  on ops.sheet_tab_snapshots (sheet_key, tab_name, captured_at desc);

-- Global knobs from sheets (shipping cost/order, return rate, AOV, etc.)
create table if not exists ops.dim_econ_assumptions (
  key text primary key,
  value_num numeric,
  value_text text,
  unit text,
  source ops.data_source not null default 'google_sheets',
  ingested_at timestamptz not null default now()
);

-- C) Offer-level economics (Unit Sell Price + Unit COG)
create table if not exists ops.dim_offer_economics (
  offer_key text primary key,        -- canonical key
  offer_name text not null,

  unit_sell_price numeric(18,2),
  unit_cogs numeric(18,4),

  is_subscription boolean not null default false,
  is_bundle boolean not null default false,

  source ops.data_source not null default 'google_sheets',
  ingested_at timestamptz not null default now()
);