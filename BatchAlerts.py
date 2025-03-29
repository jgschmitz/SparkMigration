# add this to setup alerts
df_enriched.repartition(4).write.format("mongodb") \
    .option("batchSize", 1000) \
    .mode("append") \
    .option("database", "northwind") \
    .option("collection", "orders_full") \
    .save()
