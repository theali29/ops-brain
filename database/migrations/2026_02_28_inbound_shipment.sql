BEGIN;

create table if not exists ops.fact_inbound_shipments_supply_item (
  id uuid primary key default gen_random_uuid(),

  -- the real operational grain
  supply_item_key text not null references ops.dim_supply_item(supply_item_key),

  -- timing
  arrival_month date not null,  -- YYYY-MM-01
  arrival_date date,           

  -- payload
  quantity integer not null check (quantity >= 0),

  -- metadata from sheet
  batch_name text,              -- e.g. 'B006 PT 1' (nullable for Brush/Roller)
  location_name text,           

  model_version text not null default 'sheet_v1',
  source ops.data_source not null default 'google_sheets',
  sheet_row_key text not null,  -- stable upsert key e.g. '2026-01|THM|B008' or '2026-01|BRUSH|NO_BATCH'
  ingested_at timestamptz not null default now(),

  unique (source, sheet_row_key)
);

create index if not exists inbound_supply_item_month_idx
  on ops.fact_inbound_shipments_supply_item (supply_item_key, arrival_month);

create index if not exists inbound_supply_item_ingested_idx
  on ops.fact_inbound_shipments_supply_item (ingested_at desc);

COMMIT;