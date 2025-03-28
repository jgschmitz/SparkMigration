df_readback = spark.read.format("mongodb") \
    .option("database", "northwind") \
    .option("collection", "orders_full") \
    .load()

df_readback.select("OrderID", "CustomerInfo").show()
