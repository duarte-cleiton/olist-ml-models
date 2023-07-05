with tb_join as (
  select
    distinct
    t2.idVendedor,
    t3.*

  from silver.olist.pedido t1

  left join silver.olist.item_pedido t2
  on t1.idPedido = t2.idPedido

  left join silver.olist.produto t3
  on t2.idProduto = t3.idProduto

  where t1.dtPedido < '{date}'
  and t1.dtPedido >= add_months('{date}', -6)
  and t2.idVendedor is not null
),

tb_summary as (
  select 
  idVendedor,
  avg(coalesce(nrFotos, 0)) as avgFotos,
  avg(vlComprimentoCm * vlAlturaCm * vlLarguraCm) as avgVolumeProduto,
  percentile(vlComprimentoCm * vlAlturaCm * vlLarguraCm, 0.5) as medianVolumeProduto,
  min(vlComprimentoCm * vlAlturaCm * vlLarguraCm) as minVolumeProduto,
  max(vlComprimentoCm * vlAlturaCm * vlLarguraCm) as maxVolumeProduto,

  count(distinct case when descCategoria = 'cama_mesa_banho' then idProduto end) / count(distinct idProduto) as pctCategoriacama_mesa_banho,
  count(distinct case when descCategoria = 'beleza_saude' then idProduto end) / count(distinct idProduto) as pctCategoriabeleza_saude,
  count(distinct case when descCategoria = 'esporte_lazer' then idProduto end) / count(distinct idProduto) as pctCategoriaesporte_lazer,
  count(distinct case when descCategoria = 'informatica_acessorios' then idProduto end) / count(distinct idProduto) as pctCategoriainformatica_acessorios,
  count(distinct case when descCategoria = 'moveis_decoracao' then idProduto end) / count(distinct idProduto) as pctCategoriamoveis_decoracao,
  count(distinct case when descCategoria = 'utilidades_domesticas' then idProduto end) / count(distinct idProduto) as pctCategoriautilidades_domesticas,
  count(distinct case when descCategoria = 'relogios_presentes' then idProduto end) / count(distinct idProduto) as pctCategoriarelogios_presentes,
  count(distinct case when descCategoria = 'telefonia' then idProduto end) / count(distinct idProduto) as pctCategoriatelefonia,
  count(distinct case when descCategoria = 'automotivo' then idProduto end) / count(distinct idProduto) as pctCategoriaautomotivo,
  count(distinct case when descCategoria = 'brinquedos' then idProduto end) / count(distinct idProduto) as pctCategoriabrinquedos,
  count(distinct case when descCategoria = 'cool_stuff' then idProduto end) / count(distinct idProduto) as pctCategoriacool_stuff,
  count(distinct case when descCategoria = 'ferramentas_jardim' then idProduto end) / count(distinct idProduto) as pctCategoriaferramentas_jardim,
  count(distinct case when descCategoria = 'perfumaria' then idProduto end) / count(distinct idProduto) as pctCategoriaperfumaria,
  count(distinct case when descCategoria = 'bebes' then idProduto end) / count(distinct idProduto) as pctCategoriabebes,
  count(distinct case when descCategoria = 'eletronicos' then idProduto end) / count(distinct idProduto) as pctCategoriaeletronicos

  from tb_join
  group by idVendedor
)

select
 '{date}' as dtReference,
 now() as dtIngestion,
 * 
from tb_summary
;

