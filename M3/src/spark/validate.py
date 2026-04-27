from pyspark.storagelevel import StorageLevel

def validate_data(sales, calendar, prices):

    # Cache temporal para no releer archivos varias veces
    sales.persist(StorageLevel.MEMORY_AND_DISK)
    calendar.persist(StorageLevel.MEMORY_AND_DISK)
    prices.persist(StorageLevel.MEMORY_AND_DISK)

    sales_rows = sales.count()
    calendar_rows = calendar.count()
    prices_rows = prices.count()

    print(f"Sales rows: {sales_rows}")
    print(f"Calendar rows: {calendar_rows}")
    print(f"Prices rows: {prices_rows}")

    # Duplicados
    sales_unique = sales.dropDuplicates().count()
    duplicates = sales_rows - sales_unique

    print(f"Sales sin duplicados: {sales_unique}")
    print(f"Duplicados detectados: {duplicates}")

  