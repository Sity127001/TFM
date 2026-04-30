def validate_features(df):

    print("----- VALIDATION START -----")

    # -----------------------------
    # Shape
    # -----------------------------

    print("Shape:", df.shape)

    # -----------------------------
    # Columnas esperadas
    # -----------------------------

    required_cols = [
        "sales",
        "lag_1",
        "lag_7",
        "lag_28",
        "rolling_mean_7",
        "rolling_mean_28"
    ]

    missing_cols = [
        c for c in required_cols
        if c not in df.columns
    ]

    if missing_cols:
        raise ValueError(
            f"Missing columns: {missing_cols}"
        )

    print("Column validation OK")

    # -----------------------------
    # Nulos
    # -----------------------------

    missing_ratio = (
        df.isna()
          .mean()
          .sort_values(ascending=False)
          .head(10)
    )

    print("\nTop missing columns:")
    print(missing_ratio)

    print("\n----- VALIDATION OK -----")