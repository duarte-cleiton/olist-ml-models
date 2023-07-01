-- Databricks notebook source
with tb_pedido_item as (
  select 
    t2.*,
    t1.dtPedido
  from silver.olist.pedido t1

  left join silver.olist.item_pedido t2
  on t1.idPedido = t2.idPedido

  where t1.dtPedido < '2018-01-01'
  and t1.dtPedido >= add_months('2018-01-01', -6)
  and t2.idVendedor is not null
),

tb_summary as (
  select
    idVendedor,
    count(distinct idPedido) as qtdePedidos,
    count(distinct date(dtPedido)) as qtdeDias,
    count(idProduto) as qtdeItens,
    min(datediff('2018-01-01', dtPedido)) as qtdeRecencia,
    sum(vlPreco) / count(distinct idPedido) as avgTicket,
    avg(vlPreco) as avgValorProduto,
    max(vlPreco) as maxValorProduto,
    min(vlPreco) as minValorProduto,
    count(idProduto) / count(distinct idPedido) as avgProdutoPedido

  from tb_pedido_item
  group by idVendedor
),

tb_pedido_summary as (
  select 
    idVendedor,
    idPedido,
    sum(vlPreco) as vlPreco
  from tb_pedido_item
  group by
    idVendedor,
    idPedido
),

tb_min_max as (
  select 
    idVendedor,
    min(vlPreco) as minVlPedido,
    max(vlPreco) as maxVlPedido
  from tb_pedido_summary
  group by idVendedor
),

tb_life as (
  select     
    t2.idVendedor,
    sum(t2.vlPreco) as LTV,
    max(datediff('2018-01-01', t1.dtPedido)) as qtdeDiasBase

  from silver.olist.pedido t1

  left join silver.olist.item_pedido t2
  on t1.idPedido = t2.idPedido

  where t1.dtPedido < '2018-01-01'
  and t2.idVendedor is not null
  group by t2.idVendedor
),

tb_dtpedido as (
  select 
    distinct
    idVendedor,
    date(dtPedido) as dtPedido
  from tb_pedido_item
  order by 1,2
),

tb_lag as (
  select
    *,
    lag(dtPedido) over (partition by idVendedor order by dtPedido) as lag1
  from tb_dtpedido
),

tb_intervalo as (
select
  idVendedor,
  avg(datediff(dtPedido, lag1)) as avgIntervaloVendas
from tb_lag
group by idVendedor
)

select
  '2018-01-01' as dtReference,
  t1.*,
  t2.minVlPedido,
  t2.maxVlPedido,
  t3.LTV,
  t3.qtdeDiasBase,
  t4.avgIntervaloVendas

from tb_summary t1

left join tb_min_max t2
on t1.idVendedor = t2.idVendedor

left join tb_life t3
on t1.idVendedor = t3.idVendedor

left join tb_intervalo t4
on t1.idVendedor = t4.idVendedor

;
