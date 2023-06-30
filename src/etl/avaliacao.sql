-- Databricks notebook source
with tb_pedido as (
  select 
    distinct
    t1.idPedido,
    t2.idVendedor

  from silver.olist.pedido t1

  left join silver.olist.item_pedido t2
  on t1.idPedido = t2.idPedido

  where t1.dtPedido < '2018-01-01'
  and t1.dtPedido >= add_months('2018-01-01', -6)
  and t2.idVendedor is not null
),

tb_join as (
  select 
    t1.*,
    t2.vlNota

  from tb_pedido as t1

  left join silver.olist.avaliacao_pedido t2
  on t1.idPedido = t2.idPedido
),

tb_summary as (
  select
  idVendedor,
  avg(vlNota) as avgNota,
  percentile(vlNota, 0.5) as medianNota,
  min(vlNota) as minNota,
  max(vlNota) as maxNota,
  count(vlNota) / count(idPedido) as pctAvaliacao
from tb_join
group by idVendedor
)

select 
  '2018-01-01' as dtReference,
  * 
from tb_summary
;
