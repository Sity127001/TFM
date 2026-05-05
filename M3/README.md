# TFM UOC  
## M3 — Spark ETL and Modeling Pipeline

Este módulo implementa un pipeline completo de transformación y modelado sobre 
el dataset **M5 Forecasting** utilizando **Apache Spark** y 
modelos de Machine Learning.
El objetivo principal es generar un dataset estructurado y optimizado para el 
entrenamiento de modelos predictivos de demanda, manteniendo la coherencia 
jerárquica del problema.

---

# Pipeline Overview

El flujo completo del proyecto se desarrolla en varias etapas:

## Exploratory Phase (pandas)

Validación lógica de transformaciones utilizando subconjunto del dataset.

Notebooks:

01_dataset_exploration.ipynb  
- Load raw data  
- Structural exploration  
- Dimensional analysis  
- Memory evaluation  
- Justification for Spark usage  

02_data_preparation.ipynb  
- Wide a Long transformation  
- Merge with calendar  
- Merge with prices  
- Missing value treatment  
- Export clean dataset  
- Generate sample dataset  

03_feature_engineering.ipynb  
- Temporal features  
- Lag features  
- Rolling statistics  
- Feature validation  
- Preparation for modeling  

Esta fase permite validar la lógica antes de ejecutar el procesamiento distribuido.

en esta fase se implementaron tambien exploratory scripts que hacen lo mismo lo
que se hace en notebooks de manera automatica - se han desarrollado con el fin 
de aplicarlos al dataset completo - pero se ha petado la memoria RAM
data_loading.py validate.py feature_engineering.py
---

## Spark ETL Pipeline

Al intentar procesar el dataset completo se ha petado el RAM, lo que llevo a 
desarrollo de la Implementación distribuida del pipeline completo.

Scripts:

- session.py - Spark configuration  
- load_data.py - Raw data loading  
- validate.py - Data validation  
- build_features.py - Feature generation  

Pipeline principal that run all scripts listed above:

run_spark_pipeline.py

Pasos:

- Load raw data  
- Validate datasets  
- Join calendar and prices  
- Generate lag features  
- Generate rolling statistics  
- Partition dataset by store_id  
- Export parquet dataset  

Resultado:
los ficheros parquet partitioned by store_id guardados en 

data/features/m5_features/

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

### Notebooks utilizados:

01_complete_modeling_pipeline_store_CA1.ipynb  
- Ejecución completa del pipeline de modelado sobre una tienda  
- Validación end-to-end del flujo: carga, features, entrenamiento y evaluación  

01_modeling_single_store.ipynb  
- Entrenamiento y comparación de modelos (Random Forest, XGBoost y LightGBM)  
- Evaluación mediante métricas RMSE y MAE  
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

1. run_modelo_global.py  
   - Entrenamiento del modelo global LightGBM de forma incremental por store  
   - Persistencia del modelo y lista de features  

2. run_post_analysis.py  
   - Generación de predicciones sobre todo el dataset  
   - Cálculo de importancia de variables (SHAP)  

3. build_hierarchy.py  
   - Construcción de la jerarquía completa (item, dept, cat, store, total)  
   - Agregación de predicciones por nivel  

4. run_mint_reconciliation.py  
   - Aplicación de reconciliación jerárquica (MinT aproximado por proporciones)  
   - Generación de predicciones coherentes entre niveles  

Las predicciones generadas a nivel desagregado (store–item) se utilizan posteriormente 
para reconstruir la jerarquía completa del problema y aplicar técnicas de reconciliación, 
garantizando coherencia entre los diferentes niveles.

## Modelo LSTM (extensión exploratoria)

Como complemento al modelo principal, se implementa un modelo LSTM en un entorno controlado 
utilizando un subconjunto del dataset.

Este experimento tiene como objetivo evaluar la capacidad de modelos secuenciales para capturar 
dependencias temporales en comparación con modelos basados en árboles.

El modelo LSTM no forma parte del pipeline principal debido a su mayor complejidad computacional 
y a su limitada escalabilidad en el contexto del problema.

### Notebook utilizado:

04_lstm_experiment.ipynb  
- Implementación de modelo LSTM  
- Evaluación comparativa frente a LightGBM  
- Análisis de viabilidad en el contexto del problema

---

# Project Structure

```
M3/
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
 │       ├── sales_clean.parquet
 │       ├── calendar_clean.parquet
 │       ├── prices_clean.parquet
 │       ├── m5_clean.parquet
 │       └── m5_clean_sample.parquet
 ├── notebooks/
 │   ├── exploratory/
 │       ├── 01_dataset_exploration.ipynb
 │       ├── 02_data_preparation.ipynb
 │       ├── 03a_feature_engineering_manual.ipynb
 │       └── 03b_feature_engineering_scripts.ipynb
 │    
 │   ├── modeling/ 
 │       ├── 01_modeling_single_store.ipynb
 │       ├── 02_shap_analysis_single_store.ipynb
 │       ├── 03_reconciliation_single_store.ipynb
 │       └── 04_lstm_experiment.ipynb
 │   ├── validation/ 
 │       └── overall_validation.ipynb
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
 │       └── run_mint_reconciliation.py
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

1. Crear environment a partir de  YAML

```
conda env create -f environment.yml
```
2. Activate environment
```
conda activate tfm
```
3. Ejecutar pipeline Spark (feature engineering):

```
python run_spark_pipeline.py

```

4. Ejecutar modelado

```
python run_modeling_pipeline.py
```
---

## Notes
Los archivos de datos generados (parquet) no se incluyen en el repositorio debido a su tamaño.

# Hardware Notes

Entorno utilizado:

Intel i5  
32 GB RAM  

Estrategia:

- Procesamiento distribuido mediante Apache Spark para el feature engineering  
- Entrenamiento de un modelo global LightGBM sobre el conjunto completo de datos  
- Uso de particionado por store_id para gestionar limitaciones de memoria  

---

# Workflow Summary

Exploración inicial (notebooks)  
↓  
Validación lógica en pandas (subset del dataset)  
↓  
Implementación modular en scripts Python  
↓  
Limitaciones de memoria detectadas  
↓  
Migración a Spark (feature engineering distribuido)  
↓  
Generación de dataset final en formato parquet (~58M filas)  
↓  
Entrenamiento incremental de modelo global (LightGBM)  
↓  
Generación de predicciones sobre todo el dataset  
↓  
Interpretabilidad del modelo (SHAP)  
↓  
Construcción de jerarquía completa de series temporales  

TOTAL  
 └── STORE  
      └── CAT  
           └── DEPT  
                └── ITEM  

↓  
Reconciliación jerárquica (MinT) para garantizar coherencia entre niveles  
↓  
Visualización final de resultados (Power BI)