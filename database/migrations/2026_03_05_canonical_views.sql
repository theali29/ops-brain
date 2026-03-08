-- Canonical Views


-- Inventory snapshot view
create or replace view ops.v_inventory_latest_snapshot as
with latest as (
  select max(snapshot_date) as snapshot_date
  from ops.fact_inventory_snapshot
)
select
  s.snapshot_date,
  m.supply_item_key,
  sum(s.available * m.units_per_sku)::numeric as units_on_hand
from ops.fact_inventory_snapshot s
join ops.dim_supply_item_sku_map m
  on m.sku = s.sku
cross join latest
where s.snapshot_date = latest.snapshot_date
group by s.snapshot_date, m.supply_item_key;


-- Inbound shipments 
create or replace view ops.v_inbound_supply_item_current as
select
  supply_item_key,
  arrival_month::date as month,
  sum(quantity)::numeric as inbound_units
from ops.fact_inbound_shipments_supply_item
where model_version = 'sheet_v1'
group by supply_item_key, arrival_month;


-- Demand forecast
create or replace view ops.v_demand_forecast_supply_item_current as
select
  supply_item_key,
  month::date as month,

  sum(forecast_total_units)::numeric as demand_units,
  sum(forecast_new_customer_units)::numeric as new_customer_units,
  sum(forecast_renewal_units)::numeric as renewal_units,
  sum(forecast_rebill_3m_units)::numeric as rebill_3m_units,
  sum(forecast_rebill_1m_units)::numeric as rebill_1m_units

from ops.fact_demand_forecast_supply_item
where model_version = 'sheet_v1'
group by supply_item_key, month;


-- Reorder rules
create or replace view ops.v_reorder_rules_current as
select
  supply_item_key,
  lead_time_months::numeric,
  safety_buffer_months::numeric,
  reorder_trigger_months::numeric,
  min_runway_trigger_months::numeric,
  standard_order_qty::numeric,
  source,
  ingested_at
from ops.dim_reorder_rules;