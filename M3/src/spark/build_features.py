from pyspark.sql.functions import expr, col, lag, avg, broadcast
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

    sales_long = sales.selectExpr(*sales.columns[:6], expr_str)

    # --------------------------------------------------
    # 2. Persist datasets reutilizados
    # --------------------------------------------------
    sales_long.persist(StorageLevel.MEMORY_AND_DISK)
    prices.persist(StorageLevel.MEMORY_AND_DISK)

    # --------------------------------------------------
    # 3. JOINS optimizados
    # calendar es pequeño -> broadcast
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
    # 4. Tipos
    # --------------------------------------------------
    df = df.withColumn("date", col("date").cast("date"))

    # --------------------------------------------------
    # 5. Repartition antes de ventanas
    # mejora para item/store
    # --------------------------------------------------
    df = df.repartition("store_id", "item_id")

    # --------------------------------------------------
    # 6. Window features
    # --------------------------------------------------
    window = Window.partitionBy(
        "item_id",
        "store_id"
    ).orderBy("date")

    # Lag 7 días
    df = df.withColumn(
        "lag_7",
        lag("sales", 7).over(window)
    )

    # Media móvil 28 días previos
    rolling_window = window.rowsBetween(-28, -1)

    df = df.withColumn(
        "rolling_mean_28",
        avg("sales").over(rolling_window)
    )

    # --------------------------------------------------
    # 7. Resultado final
    # --------------------------------------------------
    return df