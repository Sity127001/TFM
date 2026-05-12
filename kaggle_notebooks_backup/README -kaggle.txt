# TFM UOC  
## M3 — Spark ETL and Modeling Pipeline Kaggle

M3 — Pipeline ETL Distribuido y Forecasting Jerárquico Kaggle

Este módulo implementa un pipeline completo de transformación, modelado y reconciliación jerárquica sobre el dataset M5 Forecasting utilizando Apache Spark, Python y modelos de Machine Learning.

El objetivo principal del proyecto es construir un sistema escalable capaz de procesar grandes volúmenes de datos de series temporales retail, generar predicciones de demanda y mantener la coherencia jerárquica entre distintos niveles de agregación.

El pipeline integra:

Procesamiento distribuido mediante Apache Spark
Ingeniería de características temporales
Entrenamiento incremental de modelos predictivos
Forecasting jerárquico multiserie
Interpretabilidad mediante SHAP
Reconstrucción jerárquica multinivel
Reconciliación jerárquica aproximada mediante MinT selectivo
Evaluación jerárquica mediante WRMSSE
Persistencia parquet orientada a escalabilidad
Exportación estructurada de resultados para visualización y análisis
Integración analítica con Power BI y DuckDB
---

# Pipeline Overview

El flujo completo del proyecto se divide en varias etapas:

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

## Pipeline principal que ejecuta todos los scripts de Carga de datos y creacion e features:

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

## Modeling

### Notebook de modelado ejecutado en Kaggle:

El notebook principal ejecuta de forma secuencial todas las etapas del pipeline de modelado global y reconciliación jerárquica.

Durante esta fase se realizan:

- Entrenamiento incremental del modelo global LightGBM
- División temporal train/validation/test
- Evaluación temporal respetando estructura secuencial de la serie
- Evaluación mediante métricas RMSE y MAE
- Evaluación jerárquica mediante WRMSSE
- Reconstrucción jerárquica multinivel
- Reconciliación MinT selectiva sobre top revenue
- Análisis SHAP global
- Persistencia de resultados y artefactos

Las predicciones generadas a nivel desagregado (store–item) se utilizan 
posteriormente para reconstruir la jerarquía completa del problema y aplicar 
técnicas de reconciliación jerárquica (MinT), garantizando coherencia entre los 
diferentes niveles.

### Pipeline de modelado:

1. run_modelo_global.py  
   - Entrenamiento del modelo global LightGBM de forma incremental por store  
   - Persistencia del modelo y lista de features  

2. run_post_analysis.py  
   - Generación de predicciones sobre todo el dataset 
 
3. run_shap_analysis.py
   - Cálculo de importancia de variables (SHAP)  

4. build_hierarchy.py  
   - Construcción de la jerarquía completa (item, dept, cat, store, total)  
   - Agregación de predicciones por nivel  

5. build_m5_evaluation.py  
   - Construcción de estructuras auxiliares para evaluación jerárquica  
   - Cálculo de escalas RMSSE  
   - Generación de pesos basados en volumen de ventas  

4. run_mint_top_revenue.py  
   - Aplicación de reconciliación jerárquica (MinT aproximado por proporciones)  
   - Generación de predicciones coherentes entre niveles 

6. compute_wrmsse_final.py  
   - Cálculo final de la métrica WRMSSE  
   - Integración de predicciones reconciliadas, escalas y pesos  
   - Evaluación jerárquica global del sistema predictivo

Las predicciones generadas a nivel desagregado (store–item) se utilizan posteriormente 
para reconstruir la jerarquía completa del problema y aplicar técnicas de reconciliación, 
garantizando coherencia entre los diferentes niveles.

## Modelo LSTM (extensión exploratoria)

Como complemento al modelo principal, se implementa un modelo LSTM experimental sobre subconjuntos agregados del dataset.

Este experimento tiene como objetivo evaluar la capacidad de modelos secuenciales para capturar dependencias temporales frente a modelos basados en árboles.

El análisis se realizó utilizando series agregadas por store_id en un entorno controlado debido al elevado coste computacional asociado al entrenamiento secuencial sobre la totalidad del dataset M5.

El modelo LSTM no forma parte del pipeline principal debido a:

Mayor coste computacional
Menor escalabilidad
Mayor complejidad de entrenamiento

### Script utilizado:

run_all_lstm_experiment.ipynb  
- Implementación de modelo LSTM  
- Evaluación comparativa frente a LightGBM  
- Análisis de viabilidad en el contexto del problema
---
### Restricciones computacionales

El proyecto se desarrolló principalmente sobre la plataforma Kaggle utilizando notebooks persistentes y almacenamiento temporal asociado al entorno de ejecución.

Debido al tamaño del dataset y a la complejidad del forecasting jerárquico, fue necesario adoptar distintas estrategias de optimización:

Procesamiento distribuido con Spark
Persistencia parquet
Entrenamiento incremental
Particionado por store_id
Reconciliación MinT selectiva
Uso parcial de SHAP sobre muestras representativas
Persistencia de checkpoints y artefactos intermedios

Estas estrategias permitieron construir un pipeline reproducible y funcional sobre el dataset completo M5 Forecasting.


### El proyecto aporta:

# Contribuciones principales

El proyecto aporta:
Revisa esto bla bla par a f33
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

# Project Structure (Kaggle)
La estructura de las carpetas genera mediante scripts guardados en el notebook  m5-full-pipeline.ipynb
```
kaggle/
 working/
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
     ├── notebooks/
     │    │   ├── modeling/ 
     │             └── m5-full-pipeline.ipynb
     ├── src/
     │    ├── spark/
     │       ├── session.py
     │       ├── load_data.py
     │       ├── validate.py
     │       └── build_features.py
     │    
     │   ├── models/
     │       ├── run_modelo_global.py
     │       ├── run_post_analysis.py
     │       ├── run_shap_analysis.py
     │       ├── build_hierarchy.py
     │       ├── build_m5_evaluation.py
     │       ├── run_mint_reconciliation.py
     │       └── compute_wrmsse_final.py
     │             
     ├── run_all_spark.py
     ├── run_all_modeling.py
     ├── run_all_lstm.py
     ├── .gitignore
     └── README.md
          ...
 ```
---

## Execution

La ejecución del proyecto se realiza completamente desde la plataforma Kaggle:


## 1. Execution versión Kaggle:

La ejecución del proyecto se realiza desde la plataforma de Kaggle.
 
1. Se debe acceder al siguiente dataset público a través de este enlace:
```
https://www.kaggle.com/datasets/sity127uoc/m5-uoc
```
2.1 Acceder a la versión ejecutada guardada en Kaggle del notebook desde el enlace:
```
https://www.kaggle.com/code/sity127uoc/m5-full-pipeline-e1ed3d
```
2.2 O descargar el notebook m5-full-pipeline.ipynb  desde este repositorio (kaggle_notebooks_backup\)

subirlo al dataset https://www.kaggle.com/datasets/sity127uoc/m5-uoc

3. run all 

4. Descargar los ficheros resultantes qye se utilizan como origen de datos para el modelo semantico de power bi o el proyecto completo (no recomendado por elevado tamaño del dataset y los outputs generados).
---

## 2. Ejecución del notebook ETL para integración de ficheros Parquet 
utilizados en Power BI

La ejecución del proyecto se realiza desde la plataforma de Kaggle. 

1.Se debe acceder al siguiente dataset público a través de este enlace:
```
https://www.kaggle.com/datasets/sity127uoc/m5-uoc-powerbi-etl
```
2.1 Acceder a la versión ejecutada guardada en Kaggle del notebook desde el enlace:
```
https://www.kaggle.com/code/sity127uoc/notebookf35bddb609
```
2.2 O descargar el notebook m5-powerbi-etl.ipynb  desde este repositorio (kaggle_notebooks_backup\)

subirlo al dataset 
```
https://www.kaggle.com/datasets/sity127uoc/m5-uoc-powerbi-etl
```
3. run all

4. Descargar tablas de hechos y dimensiones agregadas
---




