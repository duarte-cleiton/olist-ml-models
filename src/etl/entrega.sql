with tb_pedido as (
  select
    t1.idPedido,
    t2.idVendedor,
    t1.descSituacao,
    t1.dtPedido,
    t1.dtAprovado,
    t1.dtEntregue,
    t1.dtEstimativaEntrega,
    sum(t2.vlFrete) as totalFrete
  from silver.olist.pedido t1

  left join silver.olist.item_pedido t2
  on t1.idPedido = t2.idPedido

  where dtPedido < '{date}'
  and dtPedido >= add_months('{date}', -6)
  and idVendedor is not null
  group by t1.idPedido,
    t2.idVendedor,
    t1.descSituacao,
    t1.dtPedido,
    t1.dtAprovado,
    t1.dtEntregue,
    t1.dtEstimativaEntrega
)

select 
  '{date}' as dtReference,
  now() as dtIngestion,
  idVendedor,
  count(distinct case when date(coalesce(dtEntregue, '{date}')) > date(dtEstimativaEntrega) then idPedido end) / count(distinct case when descSituacao = 'delivered' then idPedido end) as pctPedidoAtraso,
  count(distinct case when descSituacao = 'canceled' then idPedido end) / count(distinct idPedido) as pctPedidoCancelado,
  avg(totalFrete) as avgFrete,
  percentile(totalFrete, 0.5) as medianFrete,
  max(totalFrete) as maxFrete,
  min(totalFrete) as minFrete,
  avg(datediff(coalesce(dtEntregue, '{date}'), dtAprovado)) as qtdeDiasAprodadoEntrega,
  avg(datediff(coalesce(dtEntregue, '{date}'), dtPedido)) as qtdeDiasPedidoEntrega,
  avg(datediff(dtEstimativaEntrega, coalesce(dtEntregue, '{date}'))) as qtdeDiasEntregaPromessa

from tb_pedido
group by idVendedor
;