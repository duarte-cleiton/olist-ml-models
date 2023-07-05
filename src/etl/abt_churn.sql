-- Databricks notebook source
with tb_activate as (
  select
    idVendedor,
    min(date(dtPedido)) as dtAtivacao
  from silver.olist.pedido t1

  left join silver.olist.item_pedido t2
  on t1.idPedido = t2.idPedido

  where dtPedido >= '2018-01-01'
  and dtPedido <= date_add('2018-01-01', 45)
  and idVendedor is not null

  group by idVendedor
)

select 
  t1.*,
  t2.*,
  t3.*,
  t5.*,
  t6.*,
  case when t7.idVendedor is null then 1 else 0 end as flChurn
from silver.analytics.fs_vendedor_vendas t1

left join silver.analytics.fs_vendedor_entrega t2
on t1.idVendedor = t2.idVendedor
and t1.dtReference = t2.dtReference

left join silver.analytics.fs_vendedor_avaliacao t3
on t1.idVendedor = t3.idVendedor
and t1.dtReference = t3.dtReference

left join silver.analytics.fs_vendedor_cliente t4
on t1.idVendedor = t4.idVendedor
and t1.dtReference = t4.dtReference

left join silver.analytics.fs_vendedor_pagamentos t5
on t1.idVendedor = t5.idVendedor
and t1.dtReference = t5.dtReference

left join silver.analytics.fs_vendedor_produto t6
on t1.idVendedor = t6.idVendedor
and t1.dtReference = t6.dtReference

left join tb_activate t7
on t1.idVendedor = t7.idVendedor
and datediff(t7.dtAtivacao, t1.dtReference) + t1.qtdeRecencia <= 45

where t1.qtdeRecencia <= 45
;

-- COMMAND ----------

select * from silver.analytics.fs_vendedor_vendas
