-- Databricks notebook source
SELECT * FROM silver.olist.item_pedido as t1;

-- COMMAND ----------

WITH tb_pedidos AS (
  SELECT 
    DISTINCT
    t1.idPedido,
    t2.idVendedor
  FROM silver.olist.pedido t1

  LEFT JOIN silver.olist.item_pedido t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  AND t2.idVendedor IS NOT NULL

),


tb_join AS (
  SELECT 
    t3.idVendedor,
    t2.*
  FROM tb_pedidos t1

  LEFT JOIN silver.olist.pagamento_pedido t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.item_pedido t3
  ON t1.idPedido = t3.idPedido
),

tb_group AS (
  SELECT
    idVendedor,
    descTipoPagamento,
    COUNT(DISTINCT idPedido) as qtdePedidoMeioPagamento,
    SUM(vlPagamento) as vlPedidoMeioPagamento
  FROM tb_join
  GROUP BY
    idVendedor,
    descTipoPagamento
  ORDER BY
    idVendedor,
    descTipoPagamento
),

tb_summary as (
  SELECT  
  idVendedor,
  sum(case when descTipoPagamento = 'boleto'then qtdePedidoMeioPagamento else 0 end) as qtd_boleto,
  sum(case when descTipoPagamento = 'credit_card'then qtdePedidoMeioPagamento else 0 end) as qtd_credit_card,
  sum(case when descTipoPagamento = 'voucher'then qtdePedidoMeioPagamento else 0 end) as qtd_debit_card,
  sum(case when descTipoPagamento = 'debit_card'then qtdePedidoMeioPagamento else 0 end) as qtd_voucher,

  sum(case when descTipoPagamento = 'boleto'then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_boleto,
  sum(case when descTipoPagamento = 'credit_card'then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_credit_card,
  sum(case when descTipoPagamento = 'voucher'then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_debit_card,
  sum(case when descTipoPagamento = 'debit_card'then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_voucher,

  sum(case when descTipoPagamento = 'boleto'then vlPedidoMeioPagamento else 0 end) as valor_boleto,
  sum(case when descTipoPagamento = 'credit_card'then vlPedidoMeioPagamento else 0 end) as valor_credit_card,
  sum(case when descTipoPagamento = 'voucher'then vlPedidoMeioPagamento else 0 end) as valor_debit_card,
  sum(case when descTipoPagamento = 'debit_card'then vlPedidoMeioPagamento else 0 end) as valor_voucher,

  sum(case when descTipoPagamento = 'boleto'then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_boleto,
  sum(case when descTipoPagamento = 'credit_card'then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_credit_card,
  sum(case when descTipoPagamento = 'voucher'then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_debit_card,
  sum(case when descTipoPagamento = 'debit_card'then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_voucher
FROM tb_group
GROUP BY idVendedor
),

tb_cartao as (
  SELECT
    idVendedor,
    AVG(nrParcelas) AS avgQtdeParcelas,
    PERCENTILE(nrParcelas, 0.5) AS medianQtdeParcelas,
    MAX(nrParcelas) AS maxQtdeParcelas,
    MIN(nrParcelas) AS minQtdeParcelas
  FROM tb_join
  WHERE descTipoPagamento = 'credit_card'
  GROUP BY idVendedor
)

SELECT 
  '2018-01-01' as dtReference,
  t1.*,
  t2.avgQtdeParcelas,
  t2.medianQtdeParcelas,
  t2.maxQtdeParcelas,
  t2.minQtdeParcelas
FROM tb_summary t1
LEFT JOIN tb_cartao t2
ON t1.idVendedor = t2.idVendedor
;

