-- Create ops.dim_supply_item with keys: THM, BRUSH, ROLLER
-- Store reorder rules keyed by supply_item_key
-- Add a mapping table from supply_item_key → sku

begin;
-- 1) Canonical replenishment items
create table if not exists ops.dim_supply_item (
  supply_item_key text primary key,
  description text,
  is_active boolean not null default true,
  created_at timestamptz not null default now()
);

insert into ops.dim_supply_item (supply_item_key, description)
values
  ('THM', 'Hair Multiplier Serum'),
  ('ROLLER', 'Titanium Dermaroller'),
  ('BRUSH', 'Rosewood Brush')
on conflict do nothing;

-- 2) Replace reorder rules table (now supply-item keyed)
drop table if exists ops.dim_reorder_rules;

create table ops.dim_reorder_rules (
  supply_item_key text primary key
    references ops.dim_supply_item(supply_item_key)
    on delete cascade,

  lead_time_months numeric(6,2) not null check (lead_time_months >= 0),
  safety_buffer_months numeric(6,2) not null check (safety_buffer_months >= 0),
  reorder_trigger_months numeric(6,2) not null check (reorder_trigger_months >= 0),
  standard_order_qty integer not null check (standard_order_qty >= 0),
  min_runway_trigger_months numeric(6,2),

  source ops.data_source not null default 'google_sheets',
  ingested_at timestamptz not null default now()
);

-- 3) Supply item to SKU mapping
create table if not exists ops.dim_supply_item_sku_map (
  supply_item_key text not null
    references ops.dim_supply_item(supply_item_key)
    on delete cascade,

  sku text not null
    references ops.dim_sku(sku)
    on delete cascade,

  primary key (supply_item_key, sku)
);

insert into ops.dim_supply_item_sku_map (supply_item_key, sku) values
  ('THM', 'HAIRMULTIPLIER-1PC'),
  ('THM', 'HAIRMULTIPLIER-3PC'),
  ('ROLLER', '1PC-DERMAROLLER'),
  ('BRUSH', '1PC-ROSEWOODBRUSH')
on conflict do nothing;

commit;