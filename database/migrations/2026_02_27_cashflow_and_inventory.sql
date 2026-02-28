

BEGIN;

create table if not exists ops.fact_demand_forecast_supply_item (
  id uuid primary key default gen_random_uuid(),
  supply_item_key text not null references ops.dim_supply_item(supply_item_key),
  month date not null,

  forecast_new_customer_units integer not null default 0,
  forecast_renewal_units integer not null default 0,
  forecast_rebill_3m_units integer not null default 0,
  forecast_rebill_1m_units integer not null default 0,
  forecast_total_units integer not null default 0,

  model_version text not null default 'sheet_v1',
  source ops.data_source not null default 'google_sheets',
  sheet_row_key text not null,
  ingested_at timestamptz not null default now(),

  unique (source, supply_item_key, month, model_version)
);

create index if not exists demand_forecast_supply_item_idx
  on ops.fact_demand_forecast_supply_item (supply_item_key, month);

create table if not exists ops.fact_cashflow_plan (
  id uuid primary key default gen_random_uuid(),
  month date not null,
  metric_name text not null,
  amount numeric(18,2) not null default 0,

  model_version text not null default 'sheet_v1',
  source ops.data_source not null default 'google_sheets',
  sheet_row_key text not null,
  ingested_at timestamptz not null default now(),

  unique (source, month, metric_name, model_version)
);

create index if not exists cashflow_plan_month_idx
  on ops.fact_cashflow_plan(month);

COMMIT;