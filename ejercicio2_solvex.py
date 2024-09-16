# Paola De León
# Prueba técnica Solvex
# Ejercicio 2: Procesamiento de datos con Spark y conjunto de datos de vuelos

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Sesión de Spark
spark = SparkSession.builder.appName("Analisis de Vuelos 2015").getOrCreate()

# Cargar data de todos los meses
# IMPORTANTE: Descargar data en el siguiente link: https://we.tl/t-LID8Gsw3gL
path = "./vuelos2015/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2015_*.csv" 
df = spark.read.csv(path, header=True, inferSchema=True)


# ---------------------------------------------------- Ejercicio 2.C --------------------------------------------------- #
print("\n\n • Inciso C: Calcula la cantidad promedio de retrasos en la llegada de vuelos en un aeropuerto específico.")
aeropuerto = 'JFK'

# Filtrar los datos para aeropuerto y calcular retraso promedio
delay_avg = df.filter(df["Dest"] == aeropuerto) \
    .agg(F.avg("ArrDelay") \
    .alias("Average Arrival Delay"))

delay_avg.show()


# ---------------------------------------------------- Ejercicio 2.D --------------------------------------------------- #
print("\n\n • Inciso D: Encuentra las 10 rutas de vuelo más populares (pares de aeropuertos) en términos de la cantidad de vuelos.")
# Agrupar por "Origin" y "Dest", y cantidad de vuelos
populares = df.groupBy("Origin", "Dest") \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(10)

populares.show()
