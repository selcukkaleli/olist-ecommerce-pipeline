from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import split, explode, col, count, avg, round, sum, first, desc, row_number
from pyspark.sql.functions import from_unixtime, to_timestamp
import json

# 1. Spark session oluştur
spark = SparkSession.builder \
    .appName("Olist table joiner") \
    .getOrCreate()

# GCS credentials
spark.conf.set("google.cloud.auth.service.account.json.keyfile", 
               "/credentials/gcp-key.json")
spark.conf.set("fs.gs.impl", 
               "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
spark.conf.set("fs.AbstractFileSystem.gs.impl", 
               "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

BUCKET = "olist-ecommerce-pipeline-id-data-lake"
PROJECT = "olist-ecommerce-pipeline"

#Proje id'yi okuyalım
with open("/credentials/gcp-key.json") as f:
    creds = json.load(f)
    project_id = creds["project_id"]
    PROJECT = project_id

# 2. GCS'ten oku
print("Reading data from GCS...")
customers = spark.read.csv(
    f"gs://{BUCKET}/raw/olist/olist_customers_dataset.csv",
    header=True,
    inferSchema=True
)
geolocation = spark.read.csv(
    f"gs://{BUCKET}/raw/olist/olist_geolocation_dataset.csv",
    header=True,
    inferSchema=True
)
order_items = spark.read.csv(
    f"gs://{BUCKET}/raw/olist/olist_order_items_dataset.csv",
    header=True,
    inferSchema=True
)
payments = spark.read.csv(
    f"gs://{BUCKET}/raw/olist/olist_order_payments_dataset.csv",
    header=True,
    inferSchema=True
)
reviews = spark.read.csv(
    f"gs://{BUCKET}/raw/olist/olist_order_reviews_dataset.csv",
    header=True,
    inferSchema=True
)
orders = spark.read.csv(
    f"gs://{BUCKET}/raw/olist/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)
products = spark.read.csv(
    f"gs://{BUCKET}/raw/olist/olist_products_dataset.csv",
    header=True,
    inferSchema=True
)
sellers = spark.read.csv(
    f"gs://{BUCKET}/raw/olist/olist_sellers_dataset.csv",
    header=True,
    inferSchema=True
)
translations = spark.read.csv(
    f"gs://{BUCKET}/raw/olist/product_category_name_translation.csv",
    header=True,
    inferSchema=True
)

print(f"All tables have been read")

# 3. Aggregation for dominant payment method

# Her order_id içinde payment_value'ya göre sırala
window = Window.partitionBy("order_id").orderBy(desc("payment_value"))

# En yüksek payment_value'ya sahip satırı bul (rank=1)
payments_ranked = payments.withColumn("rank", row_number().over(window))
dominant = payments_ranked.filter(col("rank") == 1).select("order_id", "payment_type")

# Toplam ödeme tutarını hesapla
total = payments.groupBy("order_id").agg(sum("payment_value").alias("total_payment_value"))

# İkisini birleştir
payments_agg = dominant.join(total, on="order_id", how="inner")



# 4. Join işlemleri
print("Joining order_items with payments...")

df = order_items.join(payments_agg, on="order_id", how="inner")
df = df.join(orders, on="order_id", how="inner")
df = df.join(sellers, on="seller_id", how="inner")
df = df.join(customers, on="customer_id", how="inner")
df = df.join(reviews, on="order_id", how="inner")
df = df.join(products, on="product_id", how="inner")
df = df.join(translations,on="product_category_name", how="inner")

# 4.5 Drop Unnecassary Columns

print("Unnecassary columns are being eliminated...")

df = df.drop("review_comment_title","review_comment_message")

# 5. Parquet olarak GCS'e yaz
print("Writing parquet to GCS...")
df.write.mode("overwrite").parquet(
    f"gs://{BUCKET}/processed/orders_enriched/"
)
print("Parquet written successfully!")


# 6. BigQuery'e yaz
print("Writing to BigQuery...")
df.write \
    .format("bigquery") \
    .option("parentProject", project_id) \
    .option("table", f"{PROJECT}.raw.orders_enriched") \
    .option("credentialsFile", "/credentials/gcp-key.json") \
    .option("partitionField", "order_purchase_timestamp") \
    .option("partitionType", "MONTH") \
    .option("clusteredFields", "product_category_name_english,order_status") \
    .option("temporaryGcsBucket", BUCKET) \
    .mode("overwrite") \
    .save()

print("Done! BigQuery write successful!")

spark.stop()