-- 1. Drop old uniqueness constraint
alter table ops.fact_inventory_snapshot
drop constraint if exists fact_inventory_snapshot_snapshot_date_sku_location_name_key;

-- 2. Add canonical location_id
alter table ops.fact_inventory_snapshot
add column if not exists location_id bigint not null;

-- 3. Create correct unique index
create unique index if not exists inv_snapshot_uniq
  on ops.fact_inventory_snapshot (snapshot_date, sku, location_id);


