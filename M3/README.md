# TFM UOC

## M3 — Spark ETL Pipeline

Este módulo implementa un pipeline de transformación sobre el dataset M5 utilizando Apache Spark.

El objetivo es generar un dataset estructurado y optimizado para el entrenamiento de modelos de predicción de demanda.
---

## Pipeline Steps

- Load raw data  
- Data validation  
- Feature engineering  
  - lag features  
  - rolling statistics  
  - calendar features  
- Join external datasets (calendar, prices)  
- Export parquet dataset  
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


TFM/
├── src/
│ └── spark/
│ ├── session.py
│ ├── load_data.py
│ ├── validate.py
│ ├── build_features.py
│
├── run_spark_pipeline.py
├── requirements.txt
├── README.md

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

