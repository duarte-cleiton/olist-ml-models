# Databricks notebook source
# MAGIC %pip install feature-engine scikit-plot

# COMMAND ----------

from sklearn import model_selection
from sklearn import tree
from sklearn import pipeline
from sklearn import metrics
import pandas as pd
import scikitplot as skplt

from feature_engine import imputation

pd.set_option('display.max_rows', 1000)

# COMMAND ----------

# DBTITLE 1,Sample Out of Time
## SAMPLE

df = spark.table("silver.analytics.abt_olist_churn").toPandas()

# Base Out of time
df_oot = df[df['dtReference'] == '2018-01-01']

# Base de Treino
df_train = df[df['dtReference']!= '2018-01-01']
df_train.shape

# COMMAND ----------

# DBTITLE 1,Definindo variáveis
df_train.head()

var_identity = ['dtReference', 'idVendedor']
target = 'flChurn'

features = df.columns.tolist()
features = list(set(features) - set(var_identity + [target]))
features.sort()

# COMMAND ----------

X_train, X_test, y_train, y_test = model_selection.train_test_split(df_train[features],
                                                           df_train[target],
                                                           train_size=0.8,
                                                           random_state=42)

print("Proporção resposta Treino:", y_train.mean())
print("Proporção resposta Teste:", y_test.mean())

# COMMAND ----------

# DBTITLE 1,Explore
X_train.isna().sum().sort_values(ascending=False)

missing_minus_100 = ['avgIntervaloVendas',
                   'pctPedidoAtraso',
                   'minNota',
                   'medianNota',
                   'avgNota',
                   'maxNota',
                   'avgVolumeProduto',
                   'qtdeDiasAprodadoEntrega',
                   'maxVolumeProduto',
                   'minVolumeProduto',
                   'medianVolumeProduto']

missing_0 = [ 'minQtdeParcelas',
              'maxQtdeParcelas',
              'medianQtdeParcelas',
              'avgQtdeParcelas']

# COMMAND ----------

# DBTITLE 1,Transform
imputer_minus_100 = imputation.ArbitraryNumberImputer(arbitrary_number=-100, variables=missing_minus_100)
imputer_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0, variables=missing_0)

# COMMAND ----------

# DBTITLE 1,Model
# este é um modelo de árvore de decisão

model = tree.DecisionTreeClassifier(min_samples_leaf=50)

# COMMAND ----------

# Criando o pipeline

model_pipeline = pipeline.Pipeline([ ("Imputer -100", imputer_minus_100),
                                     ("Imputer 0", imputer_0),
                                     ("Decision Tree", model) ])

# COMMAND ----------

# DBTITLE 1,Treino do Algoritmo
model_pipeline.fit(X_train, y_train)

# COMMAND ----------

predict = model_pipeline.predict(X_train)

probas = model_pipeline.predict_proba(X_train)
proba = probas[:,1]
probas

# COMMAND ----------

skplt.metrics.plot_roc(y_train, probas)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(y_train, probas)

# COMMAND ----------

probas_test = model_pipeline.predict_proba(X_test)

# COMMAND ----------

skplt.metrics.plot_roc(y_test, probas_test)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(y_test, probas_test)

# COMMAND ----------

probas_oot = model_pipeline.predict_proba(df_oot[features])

# COMMAND ----------

skplt.metrics.plot_roc(df_oot[target], probas_oot)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(df_oot[target], probas_oot)

# COMMAND ----------

fs_importance = model_pipeline[-1].feature_importances_

fs_cols = model_pipeline[:-1].transform(X_train.head(1)).columns.to_list()

pd.Series(fs_importance, index=fs_cols).sort_values(ascending=False)
