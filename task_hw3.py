from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round

# 1. Створюємо сесію Spark
spark = (
    SparkSession.builder.appName("Spark_HW3")
    .master("spark://192.168.64.4:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Завантажуємо датасети
users_df = spark.read.csv("users.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv("purchases.csv", header=True, inferSchema=True)
products_df = spark.read.csv("products.csv", header=True, inferSchema=True)

# 2. Очищення даних
users_clean = users_df.dropna()
purchases_clean = purchases_df.dropna()
products_clean = products_df.dropna()

print("Очищені дані (приклад х5)")
users_clean.show(5)
purchases_clean.show(5)
products_clean.show(5)

# Об'єднання таблиць --Join--
# - об'єднуємо покупки з продуктами -→ отримуємо ціну та категорію
# - об'єднуємо з користувачами -→ отримуємо вік
joined_df = purchases_clean.join(products_clean, "product_id").join(
    users_clean, "user_id"
)

spark.stop()
