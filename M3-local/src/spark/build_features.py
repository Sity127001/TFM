from pyspark.sql.functions import expr, col, lag, avg, stddev, broadcast, weekofyear
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
    # --------------------------------------------------
    df = df.repartition("store_id", "item_id")

    # --------------------------------------------------
    # 6. Window por serie item-store
    # --------------------------------------------------
    window = Window.partitionBy(
        "item_id",
        "store_id"
    ).orderBy("date")

    # --------------------------------------------------
    # 7. Lags de ventas
    # --------------------------------------------------
    df = df.withColumn("lag_1", lag("sales", 1).over(window))
    df = df.withColumn("lag_7", lag("sales", 7).over(window))
    df = df.withColumn("lag_14", lag("sales", 14).over(window))
    df = df.withColumn("lag_28", lag("sales", 28).over(window))

    # --------------------------------------------------
    # 8. Rolling windows usando solo pasado
    # --------------------------------------------------
    rolling_7 = window.rowsBetween(-7, -1)
    rolling_14 = window.rowsBetween(-14, -1)
    rolling_28 = window.rowsBetween(-28, -1)

    df = df.withColumn("rolling_mean_7", avg("sales").over(rolling_7))
    df = df.withColumn("rolling_mean_14", avg("sales").over(rolling_14))
    df = df.withColumn("rolling_mean_28", avg("sales").over(rolling_28))

    df = df.withColumn("rolling_std_7", stddev("sales").over(rolling_7))
    df = df.withColumn("rolling_std_28", stddev("sales").over(rolling_28))

    df = df.withColumn("rolling_max_28", expr("max(sales)").over(rolling_28))
    df = df.withColumn("rolling_min_28", expr("min(sales)").over(rolling_28))

    # --------------------------------------------------
    # 9. Features de precio
    # --------------------------------------------------
    df = df.withColumn("price_lag_1", lag("sell_price", 1).over(window))

    df = df.withColumn(
        "price_diff_1",
        col("sell_price") - col("price_lag_1")
    )

    df = df.withColumn(
        "price_pct_change_1",
        expr("""
            CASE
                WHEN price_lag_1 IS NULL OR price_lag_1 = 0 THEN 0
                ELSE (sell_price - price_lag_1) / price_lag_1
            END
        """)
    )

    price_avg_window = Window.partitionBy("item_id", "store_id")

    df = df.withColumn(
        "price_mean_item_store",
        avg("sell_price").over(price_avg_window)
    )

    df = df.withColumn(
        "price_norm_item_store",
        expr("""
            CASE
                WHEN price_mean_item_store IS NULL OR price_mean_item_store = 0 THEN 0
                ELSE sell_price / price_mean_item_store
            END
        """)
    )

    # --------------------------------------------------
    # 10. Features calendario adicionales
    # --------------------------------------------------
    df = df.withColumn("weekofyear", weekofyear(col("date")))

    # --------------------------------------------------
    # 11. Limpieza SOLO de features numéricas nuevas
    # --------------------------------------------------
    numeric_features_to_fill = [
        "lag_1",
        "lag_7",
        "lag_14",
        "lag_28",
        "rolling_mean_7",
        "rolling_mean_14",
        "rolling_mean_28",
        "rolling_std_7",
        "rolling_std_28",
        "rolling_max_28",
        "rolling_min_28",
        "price_lag_1",
        "price_diff_1",
        "price_pct_change_1",
        "price_mean_item_store",
        "price_norm_item_store",
        "weekofyear"
    ]

    df = df.fillna(0, subset=numeric_features_to_fill)

    # --------------------------------------------------
    # 12. Resultado final
    # --------------------------------------------------
    return df
