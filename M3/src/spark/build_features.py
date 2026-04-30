from pyspark.sql.functions import col, lag, avg, stddev, broadcast
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel


def build_features(sales, calendar, prices):

    # --------------------------------------------------
    # 1. MELT / UNPIVOT sales_train_validation
    # --------------------------------------------------

    cols = sales.columns[6:]

    expr_str = "stack({}, {}) as (d, sales)".format(
        len(cols),
        ",".join([f"'{c}', {c}" for c in cols])
    )

    sales_long = sales.selectExpr(
        *sales.columns[:6],
        expr_str
    )

    # --------------------------------------------------
    # 2. Persist datasets reutilizados
    # --------------------------------------------------

    sales_long.persist(StorageLevel.DISK_ONLY)
    prices.persist(StorageLevel.DISK_ONLY)

    # --------------------------------------------------
    # 3. JOINS optimizados
    # --------------------------------------------------

    df = sales_long.join(
        broadcast(calendar),
        "d",
        "left"
    )

    df = df.join(
        prices,
        ["store_id", "item_id", "wm_yr_wk"],
        "left"
    )

    # --------------------------------------------------
    # Persist después del join grande
    # --------------------------------------------------

    df.persist(StorageLevel.DISK_ONLY)

    # Forzar ejecución del persist
    df.count()

    # --------------------------------------------------
    # 4. Tipos
    # --------------------------------------------------

    df = df.withColumn(
        "date",
        col("date").cast("date")
    )

    # --------------------------------------------------
    # 5. Repartition antes de ventanas
    # --------------------------------------------------

    df = df.repartition("store_id")

    # --------------------------------------------------
    # 6. Window definition
    # --------------------------------------------------

    window = Window.partitionBy(
        "item_id",
        "store_id"
    ).orderBy("date")

    # --------------------------------------------------
    # 7. Lags
    # --------------------------------------------------

    df = df.withColumn(
        "lag_1",
        lag("sales", 1).over(window)
    )

    df = df.withColumn(
        "lag_7",
        lag("sales", 7).over(window)
    )

    df = df.withColumn(
        "lag_28",
        lag("sales", 28).over(window)
    )

    # --------------------------------------------------
    # 8. Rolling windows
    # --------------------------------------------------

    rolling_window_7 = window.rowsBetween(-7, -1)
    rolling_window_28 = window.rowsBetween(-28, -1)

    df = df.withColumn(
        "rolling_mean_7",
        avg("sales").over(rolling_window_7)
    )

    df = df.withColumn(
        "rolling_mean_28",
        avg("sales").over(rolling_window_28)
    )

    df = df.withColumn(
        "rolling_std_7",
        stddev("sales").over(rolling_window_7)
    )

    df = df.withColumn(
        "rolling_std_28",
        stddev("sales").over(rolling_window_28)
    )

    # --------------------------------------------------
    # 9. Liberar memoria temporal
    # --------------------------------------------------

    sales_long.unpersist()
    prices.unpersist()

    # --------------------------------------------------
    # 10. Resultado final
    # --------------------------------------------------

    return df