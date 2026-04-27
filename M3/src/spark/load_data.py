def load_data(spark, path="data/raw/"):

    sales = spark.read.csv(f"{path}/sales_train_validation.csv", header=True, inferSchema=True)
    calendar = spark.read.csv(f"{path}/calendar.csv", header=True, inferSchema=True)
    prices = spark.read.csv(f"{path}/sell_prices.csv", header=True, inferSchema=True)

    return sales, calendar, prices