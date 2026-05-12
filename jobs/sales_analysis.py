from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, round

# ШАГ 1: Создаём точку входа в Spark
# SparkSession — это как psycopg2.connect() но для Spark
# Через неё мы читаем данные и выполняем операции
spark = SparkSession.builder \
    .appName("Sales Analysis") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# ШАГ 2: Читаем CSV файл в DataFrame
# DataFrame в Spark — это как таблица в SQL
# Но данные не загружаются в память — Spark только строит план
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/spark/data/sales.csv")

# Посмотрим на структуру таблицы
print("=== Структура данных ===")
df.printSchema()

# Посмотрим на первые 5 строк
print("=== Первые 5 строк ===")
df.show(5)

# ШАГ 3: Аналитика — сколько продаж и выручки по каждой стране
# Это как SQL: SELECT country, COUNT(*), SUM(amount) GROUP BY country
print("=== Продажи по странам ===")
df.groupBy("country") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_revenue")
    ) \
    .orderBy("total_revenue", ascending=False) \
    .show()

# ШАГ 4: Какой продукт продаётся лучше всего
print("=== Топ продуктов ===")
df.groupBy("product") \
    .agg(
        count("order_id").alias("units_sold"),
        sum("amount").alias("revenue")
    ) \
    .orderBy("units_sold", ascending=False) \
    .show()

#spark.stop()

print("=== Самые активные покупатели ===")
df.groupBy('customer_id') \
    .agg(
        count('order_id').alias('total_orders'),
        sum('amount').alias('total_revenue')
    ) \
    .orderBy('total_revenue', ascending=False) \
    .show()

#шаг 5: Читаем вторую таблицу - клиенты
customers = spark.read \
    .option("header", 'true') \
    .option('inferSchema', 'true') \
    .csv('/opt/spark/data/customers.csv')

print("=== Заказы с именами клиентов ===")
#join соеденяет две таблицы по общему полю
df.join(customers, on='customer_id', how='inner') \
    .select('name', 'product', 'amount', 'country') \
    .orderBy('amount', ascending=False) \
    .show()    

spark.stop()
