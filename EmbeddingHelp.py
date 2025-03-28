df_order_details = spark.read.jdbc(url=jdbc_url, table="OrderDetails", properties=properties)

# This will require a groupBy and collect_list or struct
from pyspark.sql.functions import collect_list, struct

df_order_items = df_order_details.groupBy("OrderID").agg(
    collect_list(struct("ProductID", "Quantity", "UnitPrice")).alias("Items")
)

df_enriched = df_transformed.join(df_order_items, "OrderID", how="left")
