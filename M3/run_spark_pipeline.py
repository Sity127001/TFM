from src.spark.session import get_spark
from src.spark.load_data import load_data
from src.spark.validate import validate_data
from src.spark.build_features import build_features
from pathlib import Path


def main():

    spark = get_spark()

    # Configuración Spark
    spark.conf.set("spark.sql.debug.maxToStringFields", 200)

    # 1. Load
    sales, calendar, prices = load_data(spark)

    # 2. Validate
    validate_data(sales, calendar, prices)

    # 3. Features
    df = build_features(sales, calendar, prices)

    # 4. Save
    output_path = Path("data/features/m5_features")

    # crear carpeta si no existe
    output_path.mkdir(parents=True, exist_ok=True)

    df = df.repartition("store_id")    

    df.write \
        .mode("overwrite") \
        .partitionBy("store_id") \
        .parquet(str(output_path))

    print("PIPELINE FINALIZADO OK")
    print(f"OUTPUT PATH: {output_path}")

    print("FILES GENERATED:")

    for f in output_path.glob("*"):
        print(f.name)

    spark.stop()


if __name__ == "__main__":
    main()