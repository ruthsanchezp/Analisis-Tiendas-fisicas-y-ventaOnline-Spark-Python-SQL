from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, count,col, row_number, avg
from pyspark.sql.window import Window

# Iniciar la sesión de Spark
spark = SparkSession.builder \
    .appName("DataFrames") \
    .master("local[*]") \
    .getOrCreate()

# Leer el archivo JSON
jsonDF = spark.read.json("F:/Alumno-105/PycharmProjects/MasterBDDS/purchases.json")

# Leer el archivo CSV
csvDF = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("F:/Alumno-105/PycharmProjects/MasterBDDS/stock.csv")

##Con Python

#1. 10 productos más vendidos
ventasProductoDF = jsonDF.groupBy("product_id").count()
top10Productos = ventasProductoDF.orderBy(desc("count")).limit(10)
top10Productos.show()

#2. Calcular la cantidad total de ventas
totalVentas = jsonDF.count()
ventasPorTipoProducto = jsonDF.groupBy("item_type").agg(count("*").alias("cuenta_ventas"))
ventasPorTipoProducto = ventasPorTipoProducto.withColumn("porcentaje", (col("cuenta_ventas") / totalVentas) * 100)
ventasPorTipoProducto.show()


#3. Obtener los 3 productos mas comprados por cada tipo de producto
ventasPorProductoYTipo = jsonDF.groupBy("item_type", "product_id").count().withColumnRenamed("count", "cuenta_ventas")
especificacionVentana = Window.partitionBy("item_type").orderBy(col("cuenta_ventas").desc())
ventasPorProductoYTipo = ventasPorProductoYTipo.withColumn("rango", row_number().over(especificacionVentana))
top3ProductosPorTipo = ventasPorProductoYTipo.filter(col("rango") <= 3)
top3ProductosPorTipo.show()

#4 Obtener los productos que son más caros que la media del precio de los productos.

precioMedio = jsonDF.agg(avg("price").alias("precio_promedio")).first()["precio_promedio"]
productosCaros = jsonDF.filter(col("price") > precioMedio).orderBy(desc("price"))
productosCaros.show()

# Con SQL
jsonDF.createOrReplaceTempView("ventas")

# 1. 10 productos más vendidos
top10ProductosSQL = spark.sql("""
    SELECT product_id, COUNT(*) as total_ventas
    FROM ventas
    GROUP BY product_id
    ORDER BY total_ventas DESC
    LIMIT 10
""")
top10ProductosSQL.show()

# 2. Calcular la cantidad total de ventas y las ventas por tipo de producto con porcentaje
totalVentasSQL = spark.sql("SELECT COUNT(*) as total_ventas FROM ventas").collect()[0]["total_ventas"]
ventasPorTipoProductoSQL = spark.sql(f"""
    SELECT item_type, COUNT(*) as cuenta_ventas, (COUNT(*) / {totalVentasSQL} * 100) as porcentaje
    FROM ventas
    GROUP BY item_type
""")
ventasPorTipoProductoSQL.show()

# 3. Obtener los 3 productos más comprados por cada tipo de producto

top3ProductosPorTipoSQL = spark.sql("""
    SELECT item_type, product_id, cuenta_ventas, rango
    FROM (
        SELECT item_type, product_id, cuenta_ventas,
               ROW_NUMBER() OVER (PARTITION BY item_type ORDER BY cuenta_ventas DESC) as rango
        FROM (
            SELECT item_type, product_id, COUNT(*) as cuenta_ventas
            FROM ventas
            GROUP BY item_type, product_id
        )
    ) AS ranked_ventas
    WHERE rango <= 3
""")
top3ProductosPorTipoSQL.show()


# 4. Obtener los productos que son más caros que la media del precio de los productos
productosCarosSQL = spark.sql("""
    SELECT *, price
    FROM ventas
    WHERE price > (SELECT AVG(price) FROM ventas)
    ORDER BY price DESC
""")
productosCarosSQL.show()