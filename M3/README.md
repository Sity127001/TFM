# TFM UOC

## M3 — Spark ETL Pipeline

Este módulo implementa un pipeline de transformación sobre el dataset M5 utilizando Apache Spark.

El objetivo es generar un dataset estructurado y optimizado para el entrenamiento de modelos de predicción de demanda.
---

## Pipeline Steps
spark 
Notebook - CA_1 (validación)
- Load raw data  
- Data validation  
- Feature engineering  
  - lag features  
  - rolling statistics  
  - calendar features  
- Join external datasets (calendar, prices)  
- Export parquet dataset - partition by por store 

Pipeline 1:
Notebook - CA_1 (validación)

Pipeline 2:
Script - foreach store

Pipeline 3:
Script opcional - global dataset
---

## Output

El pipeline genera un dataset en formato parquet:

data/features/m5_features/

Structure:

- partitioned parquet files  
- _SUCCESS flag  
- compressed columnar format  
---

## Project Structure


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
 │   │   utils.py (si aparece)
 │   │
 │   ├── ml/
 │        train_single_store.py
 │        run_all_stores.py
 │        run_global_model.py
 ├── data/
 │       ├── raw/
 │       ├── features/
 │              ├── store_id=CA_1/
 │              ├── store_id=TX_1/
                ...
 │       ├── processed/
 │              ├── spark/
 │                     CA_1.parquet
 │                     CA_2.parquet
 │                     CA_3.parquet
 │                     TX_1.parquet
 │                     TX_2.parquet
 │                     TX_3.parquet
 │                     WI_1.parquet
 │                     WI_2.parquet
 │                     WI_3.parquet
 │                     CA_4.parquet
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
 │   │         03_feature_engineering.ipynb
 │   │
 │   ├── modeling/
 │   │             04_model_training_CA1.ipynb
 │   │   ├── 05_reconciliation.ipynb
 │   │   ├── 06_shap_analysis.ipynb
 │   │   ├── 07_lstm_model.ipynb
 │
 ├── run_spark_pipeline.py
 ├── requirements.txt
 ├── README.md
 ├── .gitignore
 
---

## Execution

Instalar dependencias:

```
pip install -r requirements.txt
```

Ejecutar pipeline:

```
python run_spark_pipeline.py
```
---

## Notes
Los archivos de datos (CSV y parquet) no se incluyen en el repositorio debido a su tamaño.

i5 y 32GB RAM - modelo por store 
modelo global (solo opcional)


1_dataset_exploration.ipynb

Estructura:

1. Introducción
2. Carga de datos
3. Exploración estructura
4. Análisis dimensional
5. Identificación limitaciones
6. Justificación uso Spark

Clave aquí:

mostrar problema memoria
Notebook 02 — Preparación Datos

Nombre:

02_data_preparation.ipynb

Estructura:

Introducción
Carga
Wide → Long
Evaluación computacional
Merge calendario
Merge precios
Tratamiento missing
Guardar parquet
Guardar sample
Limitaciones → Spark

Aquí nace:

la lógica del pipeline

Notebook 03 — Feature Engineering

Nombre:

03_feature_engineering.ipynb

Estructura:

1. Variables temporales
2. Lag features
3. Rolling statistics
4. Validación features
5. Preparación para modelado

Este es:

el precursor directo de Spark

Notebooks pandas → validación lógica
Spark → ejecución escalable



Notebook 01
Exploración inicial
↓
Notebook 02
Merge completo (lento)
↓
Notebook 03
Feature engineering (solo sample)
↓
Spark pipeline
Feature engineering completo
↓
Partitioned parquet por store
↓
Modelado por store

