begin;


alter table ops.dim_supply_item_sku_map
  add column if not exists units_per_sku numeric(12,4) not null default 1;


do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'units_per_sku_positive'
      and conrelid = 'ops.dim_supply_item_sku_map'::regclass
  ) then
    alter table ops.dim_supply_item_sku_map
      add constraint units_per_sku_positive
      check (units_per_sku > 0);
  end if;
end $$;


update ops.dim_supply_item_sku_map
set units_per_sku = case
  when sku = 'HAIRMULTIPLIER-1PC' then 1
  when sku = 'HAIRMULTIPLIER-3PC' then 3
  else units_per_sku
end
where supply_item_key = 'THM';

commit;