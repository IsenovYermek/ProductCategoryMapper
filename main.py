import os
os.environ['PYSPARK_PYTHON'] = r'C:\Program Files\Python311\python.exe'
os.environ['SPARK_HOME'] = r'C:\Users\Ермек\Downloads\spark-3.5.0-bin-hadoop3'
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


# Создание SparkSession
spark = SparkSession.builder.getOrCreate()

# Создание исходного датафрейма с продуктами и категориями
data = [("Продукт1", "Категория1"),
        ("Продукт2", "Категория2"),
        ("Продукт3", "Категория1"),
        ("Продукт3", "Категория2"),
        ("Продукт4", None)]

df = spark.createDataFrame(data, ["Продукт", "Категория"])

# Добавление продуктов без категорий
df = df.union(spark.createDataFrame([("Продукт6", None)], ["Продукт", "Категория"]))

# Создание датафрейма с набором всех пар "Имя продукта - Имя категории"
result = df.select("Продукт", expr("coalesce(Категория, 'Без категории') as Категория"))

result.show()
