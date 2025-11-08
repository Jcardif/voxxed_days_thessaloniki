# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "75e3016d-8f08-4dae-8541-b28aab1413e4",
# META       "default_lakehouse_name": "ZavaOps_LH",
# META       "default_lakehouse_workspace_id": "3fb4f093-35a1-4935-99cb-0517420a7fb8",
# META       "known_lakehouses": [
# META         {
# META           "id": "75e3016d-8f08-4dae-8541-b28aab1413e4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df_customers = spark.read.format("csv").option("header","true").option("inferSchema","true").load("Files/raw-data/customers.csv")
df_online_orders = spark.read.format("csv").option("header","true").option("inferSchema","true").load("Files/raw-data/online_orders.csv")
df_online_order_line_items = spark.read.format("csv").option("header","true").option("inferSchema","true").load("Files/raw-data/online_order_line_items.csv")
df_in_store_sales = spark.read.format("csv").option("header","true").option("inferSchema","true").load("Files/raw-data/in_store_sales.csv")
df_in_store_sale_line_items = spark.read.format("csv").option("header","true").option("inferSchema","true").load("Files/raw-data/in_store_sale_line_items.csv")
df_products = spark.read.format("csv").option("header","true").option("inferSchema","true").load("Files/raw-data/products.csv")
df_product_categories = spark.read.format("csv").option("header","true").option("inferSchema","true").load("Files/raw-data/product_categories.csv")
df_retail_stores = spark.read.format("csv").option("header","true").option("inferSchema","true").load("Files/raw-data/retail_stores.csv")
df_employees = spark.read.format("csv").option("header","true").option("inferSchema","true").load("Files/raw-data/employees.csv")
df_store_inventory = spark.read.format("csv").option("header","true").option("inferSchema","true").load("Files/raw-data/store_inventory.csv")
df_departments = spark.read.format("csv").option("header","true").option("inferSchema","true").load("Files/raw-data/departments.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_customers.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("bronze.customers")
df_online_orders.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("bronze.online_orders")
df_online_order_line_items.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("bronze.online_order_line_items")
df_in_store_sales.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("bronze.in_store_sales")
df_in_store_sale_line_items.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("bronze.in_store_sale_line_items")
df_products.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("bronze.products")
df_product_categories.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("bronze.product_categories")
df_retail_stores.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("bronze.retail_stores")
df_employees.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("bronze.employees")
df_store_inventory.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("bronze.store_inventory")
df_departments.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("bronze.departments")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
