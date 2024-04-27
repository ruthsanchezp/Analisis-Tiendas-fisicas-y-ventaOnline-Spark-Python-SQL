from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, count, col, row_number, avg, when, sum as sum, asc
from pyspark.sql.window import Window
from pyspark import SparkContext
from itertools import count
from pyspark.sql.functions import rank



#Spark Session
spark = SparkSession.builder.appName("ActGrupal").getOrCreate()

dfstock = spark.read\
    .option("header","true")\
    .option("inferSchema", "true")\
    .csv(r"F:\Alumno-102\PycharmProjects\MasterBDDS\pythonProject1\stock.csv")

dfventa = spark.read.json(r"F:\Alumno-102\PycharmProjects\MasterBDDS\pythonProject1\purchases.json")


##Con la API

#1. 10 productos más vendidos
ventasProductoDF = dfventa.groupBy("product_id").count()
top10Productos = ventasProductoDF.orderBy(desc("count")).limit(10)
top10Productos.show()

#2 Porcentaje de compra de cada tipo de producto (item_type)
totalVentas = dfventa.count()
ventasPorTipoProducto = dfventa.groupBy("item_type").agg(count("*").alias("cuenta_ventas"))
ventasPorTipoProducto = ventasPorTipoProducto.withColumn("porcentaje", (col("cuenta_ventas") / totalVentas) * 100)
ventasPorTipoProducto.show()


#3. Obtener los 3 productos mas comprados por cada tipo de producto
ventasPorProductoYTipo = dfventa.groupBy("item_type", "product_id").count().withColumnRenamed("count", "cuenta_ventas")
especificacionVentana = Window.partitionBy("item_type").orderBy(col("cuenta_ventas").desc())
ventasPorProductoYTipo = ventasPorProductoYTipo.withColumn("rango", row_number().over(especificacionVentana))
top3ProductosPorTipo = ventasPorProductoYTipo.filter(col("rango") <= 3)
top3ProductosPorTipo.show()

#4 Obtener los productos que son más caros que la media del precio de los productos.

precioMedio = dfventa.agg(avg("price").alias("precio_promedio")).first()["precio_promedio"]
productosCaros = dfventa.filter(col("price") > precioMedio).orderBy(desc("price"))
productosCaros.show()

#5. Encontrar los 10 productos más comprados

top10productos = dfventa.groupBy("product_id").count()
top10productos = top10productos.orderBy(col("count").desc())
top10productos.show(10)

#6.	Indicar la tienda que ha facturado más dinero.

toptiendafact = dfventa.groupBy("shop_id").agg(sum("price").alias("total_facturado"))
toptiendafact = toptiendafact.orderBy(col("total_facturado").desc())
toptiendafact.show(1)

#7. Dividir el mundo en áreas geográficas
# ¿En qué área se utiliza más PayPal?
#API

df_ventas = dfventa.withColumn(
    "Area",
    when((col("location.lon") >= -180) & (col("location.lon") < -108), "Area1")
    .when((col("location.lon") >= -108) & (col("location.lon") < -36), "Area2")
    .when((col("location.lon") >= -36) & (col("location.lon") < 36), "Area3")
    .when((col("location.lon") >= 36) & (col("location.lon") < 108), "Area4")
    .when((col("location.lon") >= 108) & (col("location.lon") <= 180), "Area5")
)

paypal_ventas = df_ventas.filter(col("payment_type") == "paypal")
uso_paypal = paypal_ventas.groupBy("Area").count().orderBy(col("count").desc())
uso_paypal.show(1)




# Con SQL
jsonDF.createOrReplaceTempView("venta")

# 1. 10 productos más vendidos
top10ProductosSQL = spark.sql("""
    SELECT product_id, COUNT(*) as total_ventas
    FROM venta
    GROUP BY product_id
    ORDER BY total_ventas DESC
    LIMIT 10
""")
top10ProductosSQL.show()

# 2. Calcular la cantidad total de ventas y las ventas por tipo de producto con porcentaje
totalVentasSQL = spark.sql("SELECT COUNT(*) as total_ventas FROM venta").collect()[0]["total_ventas"]
ventasPorTipoProductoSQL = spark.sql(f"""
    SELECT item_type, COUNT(*) as cuenta_ventas, (COUNT(*) / {totalVentasSQL} * 100) as porcentaje
    FROM venta
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
            FROM venta
            GROUP BY item_type, product_id
        )
    ) AS ranked_ventas
    WHERE rango <= 3
""")
top3ProductosPorTipoSQL.show()


# 4. Obtener los productos que son más caros que la media del precio de los productos
productosCarosSQL = spark.sql("""
    SELECT *, price
    FROM venta
    WHERE price > (SELECT AVG(price) FROM ventas)
    ORDER BY price DESC
""")
productosCarosSQL.show()


spark.sql(""
          "SELECT shop_id, COUNT(*) AS total_productos "
          "FROM venta "
          "GROUP BY shop_id "
          "ORDER BY total_productos DESC").show(1)

#5.	Indicar la tienda que ha vendido más productos.

spark.sql(""
          "SELECT product_id, COUNT(*) AS count FROM venta "
          "GROUP BY product_id "
          "ORDER BY count DESC").show(10)

#6 Indicar la tienda que ha facturado más dinero

spark.sql(""
          "SELECT shop_id, SUM(price) AS total_ventas "
          "FROM venta "
          "GROUP BY shop_id "
          "ORDER BY total_ventas DESC").show(1)


#7. Dividir el mundo en áreas geográficas
# ¿En qué área se utiliza más PayPal?
spark.sql("""
SELECT Area, product_id, total_ventas, rank
FROM (
    SELECT Area, product_id, COUNT(*) as total_ventas, RANK() OVER (PARTITION BY Area ORDER BY COUNT(*) DESC) as rank
    FROM (
        SELECT *,
            CASE 
                WHEN location.lon >= -180 AND location.lon < -108 THEN 'Area1'
                WHEN location.lon >= -108 AND location.lon < -36 THEN 'Area2'
                WHEN location.lon >= -36 AND location.lon < 36 THEN 'Area3'
                WHEN location.lon >= 36 AND location.lon < 108 THEN 'Area4'
                WHEN location.lon >= 108 AND location.lon <= 180 THEN 'Area5'
            END AS Area
        FROM venta
    )
    GROUP BY Area, product_id
)
WHERE rank <= 3
"""
).show()


#8.	Indicar los productos que no tienen stock suficiente para las compras realizadas.

spark.sql("""
SELECT Area, COUNT(*) as total_paypal
FROM (
    SELECT *,
        CASE 
            WHEN location.lon >= -180 AND location.lon < -108 THEN 'Area1'
            WHEN location.lon >= -108 AND location.lon < -36 THEN 'Area2'
            WHEN location.lon >= -36 AND location.lon < 36 THEN 'Area3'
            WHEN location.lon >= 36 AND location.lon < 108 THEN 'Area4'
            WHEN location.lon >= 108 AND location.lon <= 180 THEN 'Area5'
        END AS Area
    FROM venta
)
WHERE payment_type = 'paypal'
GROUP BY Area
ORDER BY total_paypal DESC
LIMIT 1
""").show(1)
