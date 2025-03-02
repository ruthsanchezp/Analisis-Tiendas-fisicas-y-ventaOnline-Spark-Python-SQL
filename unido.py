from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, count, col, row_number, avg, when, sum as sum, asc, rank
from pyspark.sql.window import Window
from pyspark import SparkContext
#from itertools import count


#Spark Session
spark = SparkSession.builder.appName("ActGrupal").getOrCreate()

dfstock = spark.read\
    .option("header","true")\
    .option("inferSchema", "true")\
   # .csv("F:/Alumno-105/PycharmProjects/BDDS/stock.csv")  ##
   # .csv(r"F:\Alumno-102\PycharmProjects\BDDS\pythonProject1\stock.csv")  ## 


#dfventa = spark.read.json("F:/Alumno-105/PycharmProjects/BDDS/purchases.json")
#dfventa = spark.read.json(r"F:\Alumno-102\PycharmProjects\BDDS\pythonProject1\purchases.json") 

dfventa.createOrReplaceTempView("venta")


#1. 10 productos más comprados

#API

print("Con la API - Los 10 productos màs comprados")

ventasProductoDF = dfventa.groupBy("product_id").count()
top10Productos = ventasProductoDF.orderBy(desc("count")).limit(10)
top10Productos.show()

#SQL

print("Con SQL - Los 10 productos màs comprados")

top10ProductosSQL = spark.sql("""
    SELECT product_id, COUNT(*) as total_ventas
    FROM venta
    GROUP BY product_id
    ORDER BY total_ventas DESC
    LIMIT 10
""")
top10ProductosSQL.show()

#2 Porcentaje de compra de cada tipo de producto (item_type)

#API

print("Con la API - Porcentaje de compra de cada tipo de producto")

totalVentas = dfventa.count()
ventasPorTipoProducto = dfventa.groupBy("item_type").agg(count("*").alias("cuenta_ventas"))
ventasPorTipoProducto = ventasPorTipoProducto.withColumn("porcentaje", (col("cuenta_ventas") / totalVentas) * 100)
ventasPorTipoProducto.show()

#SQL

print("Con SQL - Porcentaje de compra de cada tipo de producto")

totalVentasSQL = spark.sql("SELECT COUNT(*) as total_ventas FROM venta").collect()[0]["total_ventas"]
ventasPorTipoProductoSQL = spark.sql(f"""
    SELECT item_type, COUNT(*) as cuenta_ventas, (COUNT(*) / {totalVentasSQL} * 100) as porcentaje
    FROM venta
    GROUP BY item_type
""")
ventasPorTipoProductoSQL.show()


#3. Obtener los 3 productos mas comprados por cada tipo de producto

#API

print("Con la API - 3 productos mas comprados por cada tipo de producto")

ventasPorProductoYTipo = dfventa.groupBy("item_type", "product_id").count().withColumnRenamed("count", "cuenta_ventas")
especificacionVentana = Window.partitionBy("item_type").orderBy(col("cuenta_ventas").desc())
ventasPorProductoYTipo = ventasPorProductoYTipo.withColumn("rango", row_number().over(especificacionVentana))
top3ProductosPorTipo = ventasPorProductoYTipo.filter(col("rango") <= 3)
top3ProductosPorTipo.show()

#SQL

print("Con SQL - 3 productos mas comprados por cada tipo de producto")

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


#4 Obtener los productos que son más caros que la media del precio de los productos.

#API

print("Con la API - Productos que son más caros que la media del precio de los productos")

precioMedio = dfventa.agg(avg("price").alias("precio_promedio")).first()["precio_promedio"]
productosCaros = dfventa.filter(col("price") > precioMedio).orderBy(desc("price"))
productosCaros.show()

#SQL

print("Con SQL - Productos que son más caros que la media del precio de los productos")

productosCarosSQL = spark.sql("""
    SELECT *, price
    FROM venta
    WHERE price > (SELECT AVG(price) FROM venta)
    ORDER BY price DESC
""")
productosCarosSQL.show()


#5.	Indicar la tienda que ha vendido más productos.

#API

print("Con la API - La tienda que ha vendido más productos.")

toptienda = dfventa.groupBy("shop_id").count()
toptienda = toptienda.orderBy(col("count").desc())
toptienda.show(1)

#SQL

print("Con SQL - La tienda que ha vendido más productos.")

spark.sql(""
          "SELECT shop_id, COUNT(*) AS total_productos "
          "FROM venta "
          "GROUP BY shop_id "
          "ORDER BY total_productos DESC").show(1)

#6.	Indicar la tienda que ha facturado más dinero.
#API
print("Con la API - La tienda que ha facturado mas dinero.")

toptiendafact = dfventa.groupBy("shop_id").agg(sum("price").alias("total_facturado"))
toptiendafact = toptiendafact.orderBy(col("total_facturado").desc())
toptiendafact.show(1)

#SQL

print("Con SQL - La tienda que ha facturado mas dinero.")

spark.sql(""
          "SELECT shop_id, SUM(price) AS total_ventas "
          "FROM venta "
          "GROUP BY shop_id "
          "ORDER BY total_ventas DESC").show(1)

#7. Dividir el mundo en áreas geográficas
# ¿En qué área se utiliza más PayPal?

#API

print("Con la API - Area en que se utiliza mas PayPal")

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

#SQL

print("Con SQL - Area en que se utiliza mas PayPal")

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

# ¿Cuáles son los 3 productos más comprados en cada área?

#API

print("Con la API - 3 productos más comprados en cada área")

ventas_por_producto_area = df_ventas.groupBy("Area", "product_id").count()
windowSpec = Window.partitionBy("Area").orderBy(col("count").desc())
ranked_ventas = ventas_por_producto_area.withColumn("rank", rank().over(windowSpec))
top3_productos_area = ranked_ventas.filter(col("rank") <= 3)
top3_productos_area.show()

#SQL

print("Con la SQL - 3 productos más comprados en cada área")

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

# ¿Qué área ha facturado menos dinero?

#API

print("Con la API - Àrea que ha facturado menos dinero")

df_ventas = df_ventas.groupBy("Area").agg(sum("price").alias("total_facturado")).orderBy(col("total_facturado"))
df_ventas.show(1)

#SQL

print("Con SQL - Àrea que ha facturado menos dinero")

spark.sql("""
SELECT Area, SUM(price) as total_facturado
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
GROUP BY Area
ORDER BY total_facturado ASC
LIMIT 1
""").show(1)


#8.	Indicar los productos que no tienen stock suficiente para las compras realizadas.

#API

print("Con la API - productos que no tienen stock suficiente")
dfcruce = dfventa.groupBy(col("product_id")).count().join(dfstock, on="product_id", how="left")
dfcruce = dfcruce.withColumn("sin_stock",
                             when(col("quantity") < col("count"), True).otherwise(False)
                             )
dfcruce.filter(col("sin_stock") == True).show()

#SQL

print("Con SQL - productos que no tienen stock suficiente")

dfstock.createOrReplaceTempView("stock")

spark.sql("""
SELECT v.product_id, 
       COUNT(*) as num_ventas,
       s.quantity,
       (s.quantity < COUNT(*)) as sin_stock
FROM venta v
LEFT JOIN stock s ON v.product_id = s.product_id
GROUP BY v.product_id, s.quantity
HAVING s.quantity < COUNT(*)
""").show()
