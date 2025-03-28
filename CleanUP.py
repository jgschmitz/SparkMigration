from pymongo import MongoClient

client = MongoClient("your_atlas_uri")
client["northwind"].drop_collection("orders_full")
client["northwind"].drop_collection("orders_with_customers")
