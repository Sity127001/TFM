# TFM UOC  
## M3-local — Spark ETL and Modeling Pipeline

M3 — Pipeline ETL Distribuido y Forecasting Jerárquico

Este módulo implementa un pipeline completo de transformación, modelado y reconciliación jerárquica sobre el dataset M5 Forecasting utilizando Apache Spark, Python y modelos de Machine Learning.

El objetivo principal del proyecto es construir un sistema escalable capaz de procesar grandes volúmenes de datos de series temporales retail, generar predicciones de demanda y mantener la coherencia jerárquica entre distintos niveles de agregación.

El pipeline integra:

- Procesamiento distribuido mediante Apache Spark
- Ingeniería de características temporales
- Entrenamiento incremental de modelos predictivos
- Forecasting jerárquico multiserie
- Interpretabilidad mediante SHAP
- Reconciliación jerárquica aproximada mediante MinT
- Exportación estructurada de resultados para visualización y análisis
---

# Pipeline Overview

El flujo completo del proyecto se divide en varias etapas:

## Exploratory Phase (pandas)

Validación lógica de transformaciones utilizando subconjunto del dataset.

La primera fase del proyecto se desarrolla utilizando pandas sobre subconjuntos del dataset.

El objetivo principal de esta etapa es validar la lógica de transformación, feature engineering y modelado antes de ejecutar el procesamiento distribuido sobre el dataset completo.

Durante esta fase se analizaron:

- Estructura del dataset
- Dimensionalidad
- Uso de memoria
- Transformaciones temporales
- Variables categóricas
- Estrategias de modelado
- Viabilidad computacional

Notebooks utilizados:

01_dataset_exploration.ipynb  
- Carga de datos originales
- Exploración estructural
- Análisis dimensional
- Evaluación de memoria
- Justificación del uso de Spark  

02_data_preparation.ipynb  
- Transformación wide-to-long
- Integración con calendar.csv
- Integración con sell_prices.csv
- Tratamiento de valores nulos
- Exportación de datasets limpios
- Generación de subconjuntos de prueba  

03_feature_engineering.ipynb  
- Variables temporales
- Variables lag
- Rolling statistics
- Validación de features
- Preparación para modelado

## Exploratory Scripts

Además de los notebooks exploratorios, se desarrollaron scripts automáticos 
equivalentes para validar la ejecución del pipeline fuera del entorno notebook.
Estos scripts fueron diseñados inicialmente para ejecutarse sobre el dataset 
completo utilizando pandas. Sin embargo, durante las pruebas se detectaron 
limitaciones de memoria debido al tamaño del dataset.

Scripts exploratorios
data_loading.py
validate.py
feature_engineering.py

La detección de estas limitaciones motivó posteriormente la migración del 
pipeline ETL a Apache Spark.
---

## Spark ETL Pipeline
Debido al tamaño final del dataset (~58 millones de registros), se implementó 
un pipeline ETL distribuido mediante Apache Spark.

Esta arquitectura permite:

- Procesar el dataset completo
- Reducir el consumo de memoria local
- Escalar el feature engineering
- Exportar datasets optimizados en formato parquet

## Scripts Spark:

- session.py - Configuración de sesión Spark.  
- load_data.py - Carga de datasets originales.  
- validate.py - Validación estructural y control de calidad. 
- build_features.py - Generación distribuida de features temporales.  

## Pipeline principal that run all scripts listed above:

- run_spark_pipeline.py
Script encargado de orquestar todas las etapas ETL.

### Etapas ejecutadas
- Load raw data
- Validate datasets
- Merge calendar and prices
- Generate lag features
- Generate rolling statistics
- Partition dataset by store_id
- Export parquet dataset

### Resultado ETL:

El pipeline genera un dataset final particionado por tienda en formato parquet: 
```
data/features/m5_features/
```
---

## Modeling Strategy

Igual que en la fase de carga de datos y feature engineering, se utilizan notebooks 
para validar toda la lógica de modelado sobre un subconjunto del dataset. Estos notebooks 
permiten identificar problemas, definir variables y establecer el flujo de trabajo antes 
de trasladarlo a scripts automatizados que operan sobre el dataset completo.

Durante la fase inicial de modelado (M2), se realizó una comparación entre 
distintos algoritmos de Machine Learning sobre subconjuntos del dataset, 
incluyendo Random Forest, XGBoost y LightGBM.

Esta evaluación se llevó a cabo en notebooks utilizando una única tienda como 
caso representativo, permitiendo analizar el comportamiento de cada modelo en 
términos de precisión (RMSE, MAE), tiempo de entrenamiento y capacidad de 
generalización.

Los resultados obtenidos mostraron que LightGBM ofrecía el mejor equilibrio 
entre rendimiento y eficiencia computacional, especialmente en presencia de 
variables categóricas y datasets de gran tamaño.

Por este motivo, se selecciona LightGBM como modelo principal para su aplicación
en el pipeline global.

### Notebooks de modelado:

01_modeling_single_store.ipynb  
- Entrenamiento y comparación de modelos (Random Forest, XGBoost y LightGBM) sobre una tienda  
- División temporal train/validation/test  
- Evaluación temporal respetando estructura secuencial de la serie (Aproximación
a validación temporal rolling) 
- Evaluación mediante métricas RMSE y MAE  
- Evaluación jerárquica inspirada en WRMSSE  
- Selección del modelo más adecuado

02_shap_analysis_single_store.ipynb  
- Análisis de interpretabilidad del modelo mediante valores SHAP  
- Identificación de variables más relevantes en la predicción  

03_reconciliation_single_store.ipynb  
- Implementación inicial de reconciliación jerárquica sobre una tienda  
- Validación de coherencia entre niveles agregados  

04_lstm_experiment.ipynb  
- Implementación de un modelo LSTM como enfoque alternativo  
- Evaluación comparativa frente a modelos basados en árboles  
- Análisis de viabilidad en el contexto del problema

Debido al tamaño del dataset (~58 millones de registros), se implementa un 
modelo global basado en LightGBM, entrenado sobre el conjunto completo de datos 
que incluye múltiples tiendas y productos. Este enfoque permite capturar 
patrones compartidos entre diferentes series temporales y mejorar la capacidad 
de generalización.
Para evitar limitaciones de memoria, el entrenamiento se realiza de forma 
secuencial por particiones (store_id), manteniendo en todo momento un único 
modelo global y un espacio de características común.

Las predicciones generadas a nivel desagregado (store–item) se utilizan 
posteriormente para reconstruir la jerarquía completa del problema y aplicar 
técnicas de reconciliación jerárquica (MinT), garantizando coherencia entre los 
diferentes niveles.

### Pipeline de modelado:

1. build_m5_evaluation.py
   → crea scale.parquet y weights.parquet

2. run_modelo_global.py
   → entrena LightGBM

3. run_post_analysis.py
   → genera predicciones

4. build_hierarchy.py
   → construye jerarquía

5. run_mint_exact_top_revenue.py
   → usa weights + hierarchy + predictions

6. compute_wrmsse_final.py
   → calcula WRMSSE


Las predicciones generadas a nivel desagregado (store–item) se utilizan posteriormente 
para reconstruir la jerarquía completa del problema y aplicar técnicas de reconciliación, 
garantizando coherencia entre los diferentes niveles.

## Modelo LSTM (extensión exploratoria)

Como complemento al modelo principal, se implementa un modelo LSTM en un entorno controlado 
utilizando un subconjunto del dataset.

Este experimento tiene como objetivo evaluar la capacidad de modelos secuenciales para capturar 
dependencias temporales en comparación con modelos basados en árboles.

Sin embargo, el modelo LSTM no forma parte del pipeline principal debido a:

- Mayor coste computacional
- Menor escalabilidad
- Mayor complejidad de entrenamiento

### Notebook utilizado:

04_lstm_experiment.ipynb  
- Implementación de modelo LSTM  
- Evaluación comparativa frente a LightGBM  
- Análisis de viabilidad en el contexto del problema
---
### Restricciones computacionales

El proyecto se desarrolló sobre hardware local:

Intel i5
32 GB RAM

Debido al tamaño del dataset y a la complejidad del forecasting jerárquico, fue necesario adoptar distintas estrategias de optimización:

Procesamiento distribuido con Spark
Particionado por store_id
Exportación parquet
Entrenamiento incremental
Reconciliación MinT aproximada
Operaciones vectorizadas
Uso selectivo de SHAP sobre subconjuntos

Estas decisiones permitieron construir un pipeline funcional y reproducible sobre el dataset completo.

Contribuciones principales

### El proyecto aporta:

# Contribuciones principales

El proyecto aporta:

- Pipeline ETL distribuido mediante Apache Spark para procesamiento de grandes 
volúmenes de datos
- Ingeniería de características temporales a gran escala
- Entrenamiento incremental de un modelo global LightGBM sobre múltiples series 
temporales
- Forecasting jerárquico multiserie
- Reconstrucción jerárquica de predicciones en múltiples niveles de agregación
- Reconciliación jerárquica aproximada mediante MinT diagonal
- Evaluación jerárquica inspirada en WRMSSE
- Integración de interpretabilidad mediante SHAP
- Arquitectura modular y reproducible orientada a escalabilidad
- Integración de Spark, pandas y modelos de Machine Learning dentro de un 
pipeline unificado

# Estructura del proyecto

Los archivos de datos generados de features no se incluyen en el repositorio 
debido a su tamaño. Los ficheros raw del dataset M5 no se incluyen en el repositorio debido a limitaciones de tamaño.
Pueden descargarse desde el siguiente dataset público de Kaggle:

https://www.kaggle.com/datasets/sity127uoc/m5-uoc

Licencia: CC BY-SA 4.0.

Tampoco se incluyen los ficheros de salida en formato Parquet debido a su tamaño. 
Estos ficheros se generan automáticamente como resultado de la ejecución del pipeline.


```
TFM/
 M3-local/
     ├── data/
     │    ├── raw/
     │        ├── sales_train_validation.csv
     │        ├── sell_prices.csv
     │        ├── calendar.csv
     │    ├── features/
     │        ├── m5_features
     │             ├── store_id=CA_1/
     │             ├── store_id=CA_2/
     │                 ...
     │
     │    ├── processed/
     │       ├── exploratory/
     │             ├── sales_clean.parquet
     │             ├── calendar_clean.parquet
     │             ├── prices_clean.parquet
     │             ├── m5_clean.parquet
     │             └── m5_clean_sample.parquet
     ├── notebooks/
     │   ├── exploratory/
     │        ├── 01_dataset_exploration.ipynb
     │        ├── 02_data_preparation.ipynb
     │        ├── 03a_feature_engineering_manual.ipynb
     │        └── 03b_feature_engineering_scripts.ipynb
     │    
     │   ├── modeling/ 
     │        ├── 01_modeling_single_store.ipynb
     │        ├── 02_shap_analysis_single_store.ipynb
     │        ├── 03_reconciliation_single_store.ipynb
     │        └── 04_lstm_experiment.ipynb
     │   ├── validation/ 
     │        └── overall_validation.ipynb
     ├── src/
     │   ├── exploratory/
     │       ├── data_loading.py
     │       ├── feature_engineering.py
     │       └── validate.py
     │    
     │   ├── spark/
     │       ├── session.py
     │       ├── load_data.py
     │       ├── validate.py
     │       └── build_features.py
     │    
     │   ├── ml/
     │       ├── run_modelo_global.py
     │       ├── run_post_analysis.py
     │       ├── build_hierarchy.py
     │       ├── run_mint_reconciliation.py
     │       ├── build_m5_evaluation.py
     │       └── compute_wrmsse_final.py
     │             
     ├── run_spark_pipeline.py
     ├── run_modeling_pipeline.py
     ├── environment.yml
     ├── .gitignore
     └── README.md
 
 ```
---

## Execution

La ejecución del proyecto se realiza desde la raíz del repositorio mediante los 
siguientes comandos:

1. Crear entorno Conda

```
conda env create -f environment.yml
```
2. Activar entorno
```
conda activate tfm
```
3. Ejecutar pipeline Spark (feature engineering):

```
python run_spark_pipeline.py

```

4. Ejecutar pipeline de modelado

```
python run_modeling_pipeline.py
```
---


# Workflow Summary

Exploración inicial mediante notebooks  
↓  
Validación lógica en pandas (subset del dataset)  
↓  
Implementación modular en scripts Python  
↓  
Detección de limitaciones de memoria en procesamiento local  
↓  
Migración a Apache Spark (feature engineering distribuido)  
↓  
Generación de dataset final en formato parquet (~58M filas)  
↓  
Entrenamiento y comparación de modelos (Random Forest, XGBoost y LightGBM) 
sobre subconjuntos representativos  
↓  
Selección de LightGBM como modelo principal  
↓ 
Entrenamiento incremental de modelo global (LightGBM)  
↓  
Generación de predicciones sobre todo el dataset  
↓  
Interpretabilidad del modelo (SHAP)  
↓  
Reconstrucción de jerarquía completa de series temporales  
TOTAL  
 └── STORE  
      └── CAT  
           └── DEPT  
                └── ITEM  
↓  
Reconciliación jerárquica (MinT) para garantizar coherencia entre niveles  
↓
Evaluación jerárquica inspirada en WRMSSE  
↓
Visualización final de resultados (Power BI)