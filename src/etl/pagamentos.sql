-- Databricks notebook source
SELECT * FROM silver.olist.item_pedido as t1;

-- COMMAND ----------

WITH tb_join AS (
  SELECT 
    t2.*,
    t3.idVendedor
  FROM silver.olist.pedido t1

  LEFT JOIN silver.olist.pagamento_pedido t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.item_pedido t3
  ON t1.idPedido = t3.idPedido

  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  AND t3.idVendedor IS NOT NULL
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
)

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
;

