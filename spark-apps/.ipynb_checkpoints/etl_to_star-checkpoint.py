
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# |%%--%%| <bjWPpzcte2|QjP5RF5qhc>

spark = SparkSession.builder \
    .appName("ETL to Star Schema") \
    .config("spark.jars", "/home/jovyan/jars/postgresql-42.7.3.jar") \
    .getOrCreate()


# |%%--%%| <QjP5RF5qhc|PUApwSbP18>

jdbc_url = "jdbc:postgresql://bigdata_postgres:5432/postgres"
connection_properties = {
    "user": "bd_user",
    "password": "bd_pass",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(
        url=jdbc_url,
        table="mock_data",
        properties=connection_properties
    )

df = df.withColumn("sale_id", F.monotonically_increasing_id())

# |%%--%%| <PUApwSbP18|tqIw7VbKUM>

dim_customer = df.groupBy("sale_customer_id").agg(
        F.first("customer_first_name").alias("customer_first_name"),
        F.first("customer_last_name").alias("customer_last_name"),
        F.first("customer_age").alias("customer_age"),
        F.first("customer_email").alias("customer_email"),
        F.first("customer_country").alias("customer_country"),
        F.first("customer_postal_code").alias("customer_postal_code"),
        F.first("customer_pet_type").alias("customer_pet_type"),
        F.first("customer_pet_name").alias("customer_pet_name"),
        F.first("customer_pet_breed").alias("customer_pet_breed")
    ).withColumnRenamed("sale_customer_id", "customer_id")

# |%%--%%| <tqIw7VbKUM|uC4boIEZSG>

dim_customer.show(n=5, truncate=False)

# |%%--%%| <uC4boIEZSG|CiojCok3Wd>

dim_product = df.groupBy("sale_product_id").agg(
        F.first("product_name").alias("product_name"),
        F.first("product_category").alias("product_category"),
        F.first("product_price").alias("product_price"),
        F.first("product_weight").alias("product_weight"),
        F.first("product_color").alias("product_color"),
        F.first("product_size").alias("product_size"),
        F.first("product_brand").alias("product_brand"),
        F.first("product_material").alias("product_material"),
        F.first("product_description").alias("product_description"),
        F.first("product_rating").alias("product_rating"),
        F.first("product_reviews").alias("product_reviews"),
        F.first("product_release_date").alias("product_release_date"),
        F.first("product_expiry_date").alias("product_expiry_date")
    ).withColumnRenamed("sale_product_id", "product_id")

# |%%--%%| <CiojCok3Wd|3ph3JETeVv>

dim_product.show(n=5, truncate=False)

# |%%--%%| <3ph3JETeVv|ZX5bfCSJLQ>

dim_time = df.select(
        F.col("sale_date").alias("date")
    ).distinct().withColumn("time_id", F.monotonically_increasing_id())

dim_time = dim_time.withColumn("year", F.year("date")).withColumn("month", F.month("date")).withColumn("day", F.dayofmonth("date")).withColumn("quarter", F.quarter("date")).withColumn("dow", F.dayofweek("date"))


# |%%--%%| <ZX5bfCSJLQ|R0U3HLnoOM>

dim_time.show(n=5, truncate=False)

# |%%--%%| <R0U3HLnoOM|hJ8hftDah5>

dim_store = df.groupBy("store_name", "store_location").agg(
        F.first("store_city").alias("store_city"),
        F.first("store_state").alias("store_state"),
        F.first("store_country").alias("store_country"),
        F.first("store_phone").alias("store_phone"),
        F.first("store_email").alias("store_email")
    ).withColumn("store_id", F.monotonically_increasing_id())


# |%%--%%| <hJ8hftDah5|0wWj6Vonen>

dim_store.show(n=5, truncate=False)

# |%%--%%| <0wWj6Vonen|hTNgzPfPaC>

dim_supplier = df.groupBy("supplier_name", "supplier_country").agg(
        F.first("supplier_contact").alias("supplier_contact"),
        F.first("supplier_email").alias("supplier_email"),
        F.first("supplier_phone").alias("supplier_phone"),
        F.first("supplier_address").alias("supplier_address"),
        F.first("supplier_city").alias("supplier_city")
    ).withColumn("supplier_id", F.monotonically_increasing_id())


# |%%--%%| <hTNgzPfPaC|CjsRMOEqZO>

dim_supplier.show(n=5, truncate=False)

# |%%--%%| <CjsRMOEqZO|VOFKQko9M9>

fact_sales = df.alias("f").join(dim_customer.alias("c"),F.col("f.sale_customer_id") == F.col("c.customer_id"), "left").join(dim_product.alias("p"),F.col("f.sale_product_id") == F.col("p.product_id"), "left").join(dim_store.alias("s"),
              (F.col("f.store_name") == F.col("s.store_name")) & 
              (F.col("f.store_location") == F.col("s.store_location")), "left") \
        .join(dim_supplier.alias("sup"),
              (F.col("f.supplier_name") == F.col("sup.supplier_name")) & 
              (F.col("f.supplier_country") == F.col("sup.supplier_country")), "left") \
        .join(dim_time.alias("t"),
              F.col("f.sale_date") == F.col("t.date"), "left") \
        .select(
            F.col("f.sale_id").alias("sale_id"),
            F.col("c.customer_id"),
            F.col("p.product_id"),
            F.coalesce(F.col("s.store_id"), F.lit(-1)).alias("store_id"),
            F.coalesce(F.col("sup.supplier_id"), F.lit(-1)).alias("supplier_id"),
            F.col("t.time_id"),
            F.col("sale_quantity").alias("quantity"),
            F.col("sale_total_price").alias("total_price")
        )


# |%%--%%| <VOFKQko9M9|bEgbd09m0z>

fact_sales.show(n=5, truncate=False)

# |%%--%%| <bEgbd09m0z|HR4checfKM>

dim_customer.write.jdbc(url=jdbc_url, table="dim_customer", mode="overwrite", properties=connection_properties) 
dim_product.write.jdbc(url=jdbc_url, table="dim_product", mode="overwrite", properties=connection_properties)   
dim_store.write.jdbc(url=jdbc_url, table="dim_store", mode="overwrite", properties=connection_properties)    
dim_supplier.write.jdbc(url=jdbc_url, table="dim_supplier", mode="overwrite", properties=connection_properties)   
dim_time.write.jdbc(url=jdbc_url, table="dim_time", mode="overwrite", properties=connection_properties)  
fact_sales.write.jdbc(url=jdbc_url, table="fact_sales", mode="overwrite", properties=connection_properties)

# |%%--%%| <HR4checfKM|TqE7aJdU5j>

spark.stop()

