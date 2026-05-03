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

Validación lógica de transformaciones utilizando subconjuntos del dataset.

Notebooks:

01_dataset_exploration.ipynb  
- Load raw data  
- Structural exploration  
- Dimensional analysis  
- Memory evaluation  
- Justification for Spark usage  

02_data_preparation.ipynb  
- Wide → Long transformation  
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

---

## Spark ETL Pipeline

Implementación distribuida del pipeline completo.

Scripts:

- session.py → Spark configuration  
- load_data.py → Raw data loading  
- validate.py → Data validation  
- build_features.py → Feature generation  

Pipeline principal:

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

data/features/m5_features/

Estructura:

partitioned parquet files  
_SUCCESS flag  
compressed columnar format  

---

## Modeling Strategy

Debido al tamaño del dataset (~58 millones de registros), el entrenamiento se realiza inicialmente por subconjuntos jerárquicos (store_id).

Pipeline de modelado:

1. Selección de tienda individual (ej: CA_1)  
2. Split temporal (train / validation / test)  
3. Entrenamiento de modelos  
4. Evaluación métricas  
5. Interpretabilidad del modelo  
6. Reconciliación jerárquica  

Posteriormente:

- Entrenamiento automático foreach store  
- Posible modelo global (no ejecutado inicialmente)  
- Implementación futura de LSTM  

---

# Project Structure

```
M3/
 ├── src/
 │   ├── spark/
 │   │   session.py
 │   │   load_data.py
 │   │   validate.py
 │   │   build_features.py
 │   │
 │   ├── exploratory/
 │   │   data_loading.py
 │   │   feature_engineering.py
 │   │   validate.py
 │   │
 │   ├── ml/
 │        train_single_store.py           (trabajando...)
 │        train_all_stores.py (iteración) (trabajando...)
 │        run_global_model.py             (trabajando...)
 ├── data/
 │       ├── raw/
 │            sales_train_validation.csv
 │            sell_prices.csv
 │            calendar.csv
 │       ├── features/
 │              ├── store_id=CA_1/
 │              ├── store_id=TX_1/
                ...
 │       ├── processed/
 │              ├── spark/
 │                     CA_1.parquet (trabajando...)
 │                     CA_2.parquet (trabajando...)
 │                     CA_3.parquet (trabajando...)
 │                     ...
 │
 │              ├── exploratory/
 │                      sales_clean.parquet
 │                      calendar_clean.parquet
 │                      prices_clean.parquet
 │                      m5_clean.parquet
 │                      m5_clean_sample.parquet
 ├── notebooks/
 │   ├── exploratory/
 │   │         01_dataset_exploration.ipynb
 │   │         02_data_preparation.ipynb
 │   │         03a_feature_engineering_manual.ipynb
 │   │         03b_feature_engineering_scripts.ipynb
 │   │
 │   ├── modeling/ 
 │   │         01_modeling_single_store.ipynb
 │   │         02_shap_analysis_single_store.ipynb
 │   │         03_reconciliation_single_store.ipynb
 modelo → predicciones → reconciliación → interpretación
 │                        01_modeling_and_comparison_single_store.ipynb
 │                        02_shap_analysis_single_store.ipynb
 │                        034 a4_reconciliation_single_store.ipynb
 │                        04_lstm_model.ipynb
 │                        05_model_foreach_store.ipynb (después)   
 │
 ├── run_spark_pipeline.py
 ├── environment.yml
 ├── .gitignore
 └── README.md
 
 ```
---

## Execution

1. Crear environment desde YAML

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
python run_train_single_store.py
```
---

## Notes
Los archivos de datos generados (CSV y parquet) no se incluyen en el repositorio debido a su tamaño.

# Hardware Notes

Entorno utilizado:

Intel i5  
32 GB RAM  

Estrategia:

- Modelos entrenados por store  
- Modelo global considerado opcional  
- Dataset completo procesado mediante Spark  

---

# Workflow Summary
Exploración inicial (notebooks)
↓
Validación lógica en pandas (sample)
↓
Implementación modular en scripts Python
↓
Limitaciones de memoria detectadas
↓
Migración a Spark (feature engineering)
↓
Dataset final en parquet (~58M filas)
↓
Modelado por store (loop dinámico)
↓
Persistencia de modelos entrenados (.pkl)
↓
Interpretabilidad (SHAP sobre modelo entrenado)
↓
Reconciliación jerárquica (Bottom-Up, MinT)
↓
Visualización final (Power BI)