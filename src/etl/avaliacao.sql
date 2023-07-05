with tb_pedido as (
  select 
    distinct
    t1.idPedido,
    t2.idVendedor

  from silver.olist.pedido t1

  left join silver.olist.item_pedido t2
  on t1.idPedido = t2.idPedido

  where t1.dtPedido < '{date}'
  and t1.dtPedido >= add_months('{date}', -6)
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
  '{date}' as dtReference,
  now() as dtIngestion,
  * 
from tb_summary
;