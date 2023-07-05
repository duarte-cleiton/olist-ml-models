with tb_join as (
  select
    distinct
    t1.idPedido,
    t1.idCliente,
    t2.idVendedor,
    t3.descUF

  from silver.olist.pedido as t1

  left join silver.olist.item_pedido as t2
  on t1.idPedido = t2.idPedido

  left join silver.olist.cliente as t3
  on t1.idCliente = t3.idCliente

  where t1.dtPedido < '{date}'
  and t1.dtPedido >= add_months('{date}', -6)
  and t2.idVendedor is not null
),

tb_group as (
  select 
  idVendedor,
  count(distinct descUF) as qtdUFsPedidos,
  count(distinct case when descUF = 'SC' then idPedido end) / count(distinct idPedido) as pctPedidoSC,
  count(distinct case when descUF = 'RO' then idPedido end) / count(distinct idPedido) as pctPedidoRO,
  count(distinct case when descUF = 'PI' then idPedido end) / count(distinct idPedido) as pctPedidoPI,
  count(distinct case when descUF = 'AM' then idPedido end) / count(distinct idPedido) as pctPedidoAM,
  count(distinct case when descUF = 'RR' then idPedido end) / count(distinct idPedido) as pctPedidoRR,
  count(distinct case when descUF = 'GO' then idPedido end) / count(distinct idPedido) as pctPedidoGO,
  count(distinct case when descUF = 'TO' then idPedido end) / count(distinct idPedido) as pctPedidoTO,
  count(distinct case when descUF = 'MT' then idPedido end) / count(distinct idPedido) as pctPedidoMT,
  count(distinct case when descUF = 'SP' then idPedido end) / count(distinct idPedido) as pctPedidoSP,
  count(distinct case when descUF = 'ES' then idPedido end) / count(distinct idPedido) as pctPedidoES,
  count(distinct case when descUF = 'PB' then idPedido end) / count(distinct idPedido) as pctPedidoPB,
  count(distinct case when descUF = 'RS' then idPedido end) / count(distinct idPedido) as pctPedidoRS,
  count(distinct case when descUF = 'MS' then idPedido end) / count(distinct idPedido) as pctPedidoMS,
  count(distinct case when descUF = 'AL' then idPedido end) / count(distinct idPedido) as pctPedidoAL,
  count(distinct case when descUF = 'MG' then idPedido end) / count(distinct idPedido) as pctPedidoMG,
  count(distinct case when descUF = 'PA' then idPedido end) / count(distinct idPedido) as pctPedidoPA,
  count(distinct case when descUF = 'BA' then idPedido end) / count(distinct idPedido) as pctPedidoBA,
  count(distinct case when descUF = 'SE' then idPedido end) / count(distinct idPedido) as pctPedidoSE,
  count(distinct case when descUF = 'PE' then idPedido end) / count(distinct idPedido) as pctPedidoPE,
  count(distinct case when descUF = 'CE' then idPedido end) / count(distinct idPedido) as pctPedidoCE,
  count(distinct case when descUF = 'RN' then idPedido end) / count(distinct idPedido) as pctPedidoRN,
  count(distinct case when descUF = 'RJ' then idPedido end) / count(distinct idPedido) as pctPedidoRJ,
  count(distinct case when descUF = 'MA' then idPedido end) / count(distinct idPedido) as pctPedidoMA,
  count(distinct case when descUF = 'AC' then idPedido end) / count(distinct idPedido) as pctPedidoAC,
  count(distinct case when descUF = 'DF' then idPedido end) / count(distinct idPedido) as pctPedidoDF,
  count(distinct case when descUF = 'PR' then idPedido end) / count(distinct idPedido) as pctPedidoPR,
  count(distinct case when descUF = 'AP' then idPedido end) / count(distinct idPedido) as pctPedidoAP
  from tb_join
  group by idVendedor 
)

select  
  '{date}' as dtReference,
  now() as dtIngestion,
  *
from tb_group
;
