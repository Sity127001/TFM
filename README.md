## Versiones experimentales del proyecto

Este README.md contiene una descripción resumida de 
dos líneas del proyecto y las instrucciones de ejecución de ambos. 
La documentación detallada y específica de cada línea de desarrollo 
puede consultarse en los README.md situados dentro de las carpetas 
correspondientes: M3-kaggle y M3-local.

El repositorio incluye las dos siguientes líneas de desarrollo diferenciadas:

### 1. Versión local inicial (baseline exploratorio)

```
TFM\M3-local
```
Corresponde a la fase inicial del proyecto desarrollada principalmente en entorno local mediante notebooks, pandas y validaciones parciales sobre subconjuntos del dataset. Esta versión permitió validar:

* Transformaciones ETL
* Ingeniería de características
* Estrategias de modelado
* Reconstrucción jerárquica
* Evaluación experimental inicial

La arquitectura local constituye la base metodológica utilizada posteriormente para construir el pipeline distribuido definitivo.

### 2. Pipeline experimental Kaggle / exp02_33f

```
TFM\M3-kaggle
```

Corresponde a la versión principal consolidada del proyecto, ejecutada sobre entorno Kaggle y orientada al procesamiento completo del dataset M5 Forecasting mediante Apache Spark y LightGBM.

Esta versión incorpora:

* Pipeline ETL distribuido
* Entrenamiento incremental global
* Persistencia parquet
* Reconstrucción jerárquica completa
* Reconciliación MinT selectiva
* Evaluación WRMSSE
* SHAP global
* Exportación analítica para Power BI

El repositorio Git incluido en este proyecto contiene principalmente la versión exp02_33f_baseline y la arquitectura local utilizada durante las fases exploratorias iniciales.

Otras variantes experimentales intermedias (exp01_29f y exp03_35f) fueron utilizadas durante validaciones metodológicas y pruebas de estabilidad, pero no forman parte completa del repositorio final debido a simplificaciones realizadas para facilitar la organización y reproducibilidad del proyecto.


# 3. Ejecutar el proyecto

## 3.1 Execución: versión Local:

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

## 3.2 Execution versión Kaggle:

La ejecución del proyecto se realiza desde la plataforma de Kaggle.
 
1. Se debe acceder al siguiente dataset público a través de este enlace:

https://www.kaggle.com/datasets/sity127uoc/m5-uoc

2. Subir el notebook 

3. run all

4. Descargar proyecto con outputs
---

## 3.3 Ejecución del notebook ETL para integración de ficheros Parquet utilizados en Power BI.
Este notebook utiliza como entrada los **outputs Parquet generados 
previamente por las diferentes ejecuciones experimentales del pipeline 
predictivo.**

Estos outputs se encuentran publicados en el siguiente dataset público de 
Kaggle: https://www.kaggle.com/datasets/tatyanasilchenko/m5-etl

La ejecución del proyecto se realiza desde la plataforma de Kaggle. 
1.Se debe acceder al siguiente dataset público (licensia CC BY-SA 4.0) a través de este enlace:

https://www.kaggle.com/datasets/tatyanasilchenko/m5-etl

2. Subir el notebook m5_powerbi_modeling.ipynb (ubicado en M3-kaggle\notebooks\powerbi-etl)

3. run all

4. Descargar tablas de hechos y dimensiones agregadas
---
---
