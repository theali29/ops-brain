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

-- Offer-level economics (Unit Sell Price + Unit COG )
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