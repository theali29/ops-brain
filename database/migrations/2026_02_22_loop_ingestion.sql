create table if not exists ops.fact_loop_subscriptions (
  subscription_id            bigint primary key,
  shopify_subscription_id    bigint,
  origin_order_shopify_id    bigint,

  loop_customer_id           bigint,
  shopify_customer_id        bigint,

  status                     text not null,
  created_at_loop            timestamptz,
  updated_at_loop            timestamptz,

  paused_at                  timestamptz,
  cancelled_at               timestamptz,

  cancellation_reason        text,
  cancellation_comment       text,

  completed_orders_count     integer,

  is_prepaid                 boolean,
  is_marked_for_cancellation boolean,

  last_payment_status        text,
  currency_code              text,

  total_line_item_price              numeric(18,2),
  total_line_item_discounted_price   numeric(18,2),
  delivery_price                     numeric(18,2),

  next_billing_date_epoch    bigint,
  next_billing_at            timestamptz,

  billing_interval           text,
  billing_interval_count     integer,
  delivery_interval          text,
  delivery_interval_count    integer,

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


create table if not exists ops.fact_loop_subscription_lines (
  id                        bigserial primary key,

  subscription_id           bigint not null references ops.fact_loop_subscriptions(subscription_id) on delete cascade,
  line_id                   bigint not null,

  sku                       text references ops.dim_sku(sku),
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

create table if not exists ops.fact_loop_orders (
  loop_order_id            bigint primary key,
  shopify_order_id         bigint,
  shopify_order_number     bigint,

  status                   text,
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