from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col

# Step 1: Create Spark Session with required JARs
spark = SparkSession.builder \
    .appName("NorthwindMigration") \
    .config("spark.jars", "/path/to/sqljdbc42.jar,/path/to/mongo-spark-connector_2.12-10.0.5.jar") \
    .config("spark.mongodb.write.connection.uri", "mongodb+srv://<user>:<password>@<cluster>.mongodb.net/northwind") \
    .getOrCreate()

# Step 2: Define JDBC SQL Server connection
jdbc_url = "jdbc:sqlserver://<sql_host>:<port>;databaseName=Northwind"
properties = {
    "user": "<your_sql_user>",
    "password": "<your_sql_password>",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Step 3: Read Customers and Orders tables from SQL Server
df_customers = spark.read.jdbc(url=jdbc_url, table="Customers", properties=properties)
df_orders = spark.read.jdbc(url=jdbc_url, table="Orders", properties=properties)

# Optional: Also migrate raw data if needed
df_customers.write.format("mongodb") \
    .mode("overwrite") \
    .option("database", "northwind") \
    .option("collection", "customers") \
    .save()

df_orders.write.format("mongodb") \
    .mode("overwrite") \
    .option("database", "northwind") \
    .option("collection", "orders") \
    .save()

# Step 4: Join and transform Orders + Customer Info
df_orders_with_customers = df_orders.join(
    df_customers,
    df_orders["CustomerID"] == df_customers["CustomerID"],
    how="left"
)

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
    ).alias("CustomerInfo")
)

# Step 5: Write transformed data to MongoDB Atlas
df_transformed.write.format("mongodb") \
    .mode("overwrite") \
    .option("database", "northwind") \
    .option("collection", "orders_with_customers") \
    .save()

spark.stop()
