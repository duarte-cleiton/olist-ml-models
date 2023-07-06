-- Databricks notebook source
with tb_features as (
  select 
    t1.dtReference,
    t1.idVendedor,
    t1.qtdePedidos,
    t1.qtdeDias,
    t1.qtdeItens,
    t1.qtdeRecencia,
    t1.avgTicket,
    t1.avgValorProduto,
    t1.maxValorProduto,
    t1.minValorProduto,
    t1.avgProdutoPedido,
    t1.minVlPedido,
    t1.maxVlPedido,
    t1.LTV,
    t1.qtdeDiasBase,
    t1.avgIntervaloVendas,
    t2.pctPedidoAtraso,
    t2.pctPedidoCancelado,
    t2.avgFrete,
    t2.medianFrete,
    t2.maxFrete,
    t2.minFrete,
    t2.qtdeDiasAprodadoEntrega,
    t2.qtdeDiasPedidoEntrega,
    t2.qtdeDiasEntregaPromessa,
    t3.avgNota,
    t3.medianNota,
    t3.minNota,
    t3.maxNota,
    t3.pctAvaliacao,
    t4.qtdUFsPedidos,
    t4.pctPedidoSC,
    t4.pctPedidoRO,
    t4.pctPedidoPI,
    t4.pctPedidoAM,
    t4.pctPedidoRR,
    t4.pctPedidoGO,
    t4.pctPedidoTO,
    t4.pctPedidoMT,
    t4.pctPedidoSP,
    t4.pctPedidoES,
    t4.pctPedidoPB,
    t4.pctPedidoRS,
    t4.pctPedidoMS,
    t4.pctPedidoAL,
    t4.pctPedidoMG,
    t4.pctPedidoPA,
    t4.pctPedidoBA,
    t4.pctPedidoSE,
    t4.pctPedidoPE,
    t4.pctPedidoCE,
    t4.pctPedidoRN,
    t4.pctPedidoRJ,
    t4.pctPedidoMA,
    t4.pctPedidoAC,
    t4.pctPedidoDF,
    t4.pctPedidoPR,
    t4.pctPedidoAP,
    t5.qtd_boleto,
    t5.qtd_credit_card,
    t5.qtd_debit_card,
    t5.qtd_voucher,
    t5.pct_qtd_boleto,
    t5.pct_qtd_credit_card,
    t5.pct_qtd_debit_card,
    t5.pct_qtd_voucher,
    t5.valor_boleto,
    t5.valor_credit_card,
    t5.valor_debit_card,
    t5.valor_voucher,
    t5.pct_valor_boleto,
    t5.pct_valor_credit_card,
    t5.pct_valor_debit_card,
    t5.pct_valor_voucher,
    t5.avgQtdeParcelas,
    t5.medianQtdeParcelas,
    t5.maxQtdeParcelas,
    t5.minQtdeParcelas,
    t6.avgFotos,
    t6.avgVolumeProduto,
    t6.medianVolumeProduto,
    t6.minVolumeProduto,
    t6.maxVolumeProduto,
    t6.pctCategoriacama_mesa_banho,
    t6.pctCategoriabeleza_saude,
    t6.pctCategoriaesporte_lazer,
    t6.pctCategoriainformatica_acessorios,
    t6.pctCategoriamoveis_decoracao,
    t6.pctCategoriautilidades_domesticas,
    t6.pctCategoriarelogios_presentes,
    t6.pctCategoriatelefonia,
    t6.pctCategoriaautomotivo,
    t6.pctCategoriabrinquedos,
    t6.pctCategoriacool_stuff,
    t6.pctCategoriaferramentas_jardim,
    t6.pctCategoriaperfumaria,
    t6.pctCategoriabebes,
    t6.pctCategoriaeletronicos
    
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

  where t1.qtdeRecencia <= 45
),

tb_event as (
  select
    idVendedor,
    date(dtPedido) as dtPedido
  from silver.olist.item_pedido t1
  left join silver.olist.pedido t2
  on t1.idPedido = t2.idPedido
  where t1.idVendedor is not null
),

tb_flag as (

  select 
      t1.dtReference,
      t1.idVendedor,
      min(t2.dtPedido) as dtProxPedido

      from tb_features t1

      left join tb_event t2
      on t1.idVendedor = t2.idVendedor
      and t1.dtReference <= t2.dtPedido
      
      and datediff(dtPedido, dtReference) <= 45 - qtdeRecencia

      group by 1,2
)

select
  t1.*,
  case when dtProxPedido is null then 1 else 0 end as flChurn

  from tb_features t1

  left join tb_flag t2
  on t1.idVendedor = t2.idVendedor
  and t1.dtReference = t2.dtReference

  order by t1.idVendedor, t2.dtReference

;

-- COMMAND ----------

select * from silver.analytics.fs_vendedor_vendas order by dtReference;
