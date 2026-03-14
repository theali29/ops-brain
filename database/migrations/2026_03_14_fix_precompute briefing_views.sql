create or replace view ops.v_ops_briefing_item_current as
with latest as (
  select max(snapshot_date) as snapshot_date
  from ops.fact_inventory_snapshot
),
anchor as (
  select
    snapshot_date,
    date_trunc('month', snapshot_date)::date as anchor_month
  from latest
),
inventory as (
  select
    v.snapshot_date,
    v.supply_item_key,
    v.units_on_hand
  from ops.v_inventory_latest_snapshot v
  join latest l
    on v.snapshot_date = l.snapshot_date
),
demand_4m as (
  select
    d.supply_item_key,
    avg(d.demand_units)::numeric as avg_demand_next_4m
  from ops.v_demand_forecast_supply_item_current d
  cross join anchor a
  where d.month >= a.anchor_month
    and d.month < (a.anchor_month + interval '4 months')::date
  group by d.supply_item_key
),
inbound_6m as (
  select
    i.supply_item_key,
    sum(i.inbound_units)::numeric as inbound_next_6m
  from ops.v_inbound_supply_item_current i
  cross join anchor a
  where i.month >= a.anchor_month
    and i.month < (a.anchor_month + interval '6 months')::date
  group by i.supply_item_key
)
select
  inv.snapshot_date,
  a.anchor_month,
  inv.supply_item_key,
  inv.units_on_hand,
  round(coalesce(d4.avg_demand_next_4m, 0), 0) as avg_demand_next_4m,
  case
    when coalesce(d4.avg_demand_next_4m, 0) = 0 then null
    else round(inv.units_on_hand / d4.avg_demand_next_4m, 2)
  end as operational_runway_months,
  case
    when coalesce(d4.avg_demand_next_4m, 0) = 0 then null
    else round((inv.units_on_hand + coalesce(i6.inbound_next_6m, 0)) / d4.avg_demand_next_4m, 2)
  end as planning_runway_months,
  coalesce(i6.inbound_next_6m, 0) as inbound_next_6m,
  r.lead_time_months,
  r.safety_buffer_months,
  r.reorder_trigger_months,
  r.min_runway_trigger_months,
  r.standard_order_qty,
  case
    when inv.units_on_hand < 0 then 'STOCKOUT'
    when coalesce(d4.avg_demand_next_4m, 0) = 0 then 'NO_DEMAND'
    when (inv.units_on_hand / d4.avg_demand_next_4m) <= r.reorder_trigger_months then 'ORDER_NOW'
    when r.min_runway_trigger_months is not null
         and (inv.units_on_hand / d4.avg_demand_next_4m) <= r.min_runway_trigger_months then 'PLAN_ORDER'
    else 'OK'
  end as reorder_status
from inventory inv
cross join anchor a
left join demand_4m d4
  on d4.supply_item_key = inv.supply_item_key
left join inbound_6m i6
  on i6.supply_item_key = inv.supply_item_key
left join ops.v_reorder_rules_current r
  on r.supply_item_key = inv.supply_item_key;


create or replace view ops.v_ops_briefing_walkforward_current as
with recursive
latest as (
  select max(snapshot_date) as snapshot_date
  from ops.fact_inventory_snapshot
),
anchor as (
  select
    snapshot_date,
    date_trunc('month', snapshot_date)::date as anchor_month
  from latest
),
months as (
  select
    generate_series(
      (select anchor_month from anchor),
      ((select anchor_month from anchor) + interval '5 months')::date,
      interval '1 month'
    )::date as month
),
base as (
  select
    inv.supply_item_key,
    m.month,
    inv.units_on_hand as starting_inventory,
    coalesce(d.demand_units, 0)::numeric as demand_units,
    coalesce(i.inbound_units, 0)::numeric as inbound_units
  from ops.v_inventory_latest_snapshot inv
  join latest l
    on inv.snapshot_date = l.snapshot_date
  cross join months m
  left join ops.v_demand_forecast_supply_item_current d
    on d.supply_item_key = inv.supply_item_key
   and d.month = m.month
  left join ops.v_inbound_supply_item_current i
    on i.supply_item_key = inv.supply_item_key
   and i.month = m.month
),
walk as (
  select
    b.supply_item_key,
    b.month,
    b.starting_inventory as opening_balance,
    b.inbound_units,
    b.demand_units,
    (b.starting_inventory + b.inbound_units - b.demand_units) as closing_balance
  from base b
  join anchor a
    on b.month = a.anchor_month

  union all

  select
    b.supply_item_key,
    b.month,
    w.closing_balance as opening_balance,
    b.inbound_units,
    b.demand_units,
    (w.closing_balance + b.inbound_units - b.demand_units) as closing_balance
  from base b
  join walk w
    on b.supply_item_key = w.supply_item_key
   and b.month = (w.month + interval '1 month')::date
)
select
  supply_item_key,
  month,
  opening_balance,
  inbound_units,
  demand_units,
  closing_balance,
  case
    when closing_balance < 0 then 'NEGATIVE'
    when closing_balance = 0 then 'ZERO'
    else 'POSITIVE'
  end as month_status
from walk
order by supply_item_key, month;