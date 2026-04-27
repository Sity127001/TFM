from src.spark.session import get_spark
from src.spark.load_data import load_data
from src.spark.validate import validate_data
from src.spark.build_features import build_features

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
        # Save
    output_path = "data/features/m5_features"

    df.write.mode("overwrite").parquet(output_path)

    print("PIPELINE FINALIZADO OK")
    print(f"OUTPUT PATH: {output_path}")

    # Mostrar archivos generados
    path = Path(output_path)

    print("FILES GENERATED:")

    for f in path.glob("*"):
        print(f.name)

    spark.stop()

if __name__ == "__main__":
    main()