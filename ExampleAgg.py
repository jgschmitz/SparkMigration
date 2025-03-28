df_country_stats = df_orders_with_customers.groupBy("Country").count()
df_country_stats.show()
