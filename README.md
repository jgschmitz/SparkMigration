# SparkMigration 🧠
Here's a practical example that shows how to join and transform the Northwind Orders and Customers tables before writing the result to MongoDB.

🧠 Example: Join & Transform Before Writing to MongoDB
🗂️ Goal
Create a MongoDB collection where each document is an order, and it includes embedded customer info instead of just a CustomerID.

🔁 1. Read Tables from SQL Server
```
df_customers = spark.read.jdbc(url=jdbc_url, table="Customers", properties=properties)
df_orders = spark.read.jdbc(url=jdbc_url, table="Orders", properties=properties)
```
🔗 2. Join Orders with Customers
```
# Join on CustomerID
df_orders_with_customers = df_orders.join(
    df_customers,
    df_orders["CustomerID"] == df_customers["CustomerID"],
    how="left"
)
```
🧽 3. Select and Rename Fields (Optional)
You might want to clean up or rename some fields for a cleaner MongoDB document:
```
from pyspark.sql.functions import struct, col

df_transformed = df_orders_with_customers.select(
    col("OrderID"),
    col("OrderDate"),
    col("ShipName"),
    col("ShipCity"),
    struct(
        col("CustomerID"),
        col("CompanyName"),
        col("ContactName"),
        col("Country")
    ).alias("CustomerInfo")  # This will be an embedded document
)
```
📤 4. Write to MongoDB
df_transformed.write.format("mongodb") \
    .mode("overwrite") \
    .option("database", "northwind") \
    .option("collection", "orders_with_customers") \
    .save()
```
✅ Sample Output in MongoDB
Here’s what a sample document might look like in MongoDB after transformation:
```
{
  "OrderID": 10248,
  "OrderDate": "1996-07-04T00:00:00.000Z",
  "ShipName": "Vins et alcools Chevalier",
  "ShipCity": "Reims",
  "CustomerInfo": {
    "CustomerID": "VINET",
    "CompanyName": "Vins et alcools Chevalier",
    "ContactName": "Paul Henriot",
    "Country": "France"
  }
}
```
