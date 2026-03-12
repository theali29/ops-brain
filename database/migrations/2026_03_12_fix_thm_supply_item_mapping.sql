-- Fix THM inventory aggregation: exclude 3PC bundle SKU from physical THM stock
delete from ops.dim_supply_item_sku_map
where supply_item_key = 'THM'
  and sku = 'HAIRMULTIPLIER-3PC';