# Paola De León
# Prueba técnica Solvex
# Ejercicio 3: Integración de Pandas y Spark con datos de películas y críticas

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Sesión de Spark
spark = SparkSession.builder.appName("Combinar Pandas y Spark").getOrCreate()

# Dataframe de Pandas
datos_peliculas = pd.DataFrame({
    'ID': [1, 2, 3, 4],
    'Título': ['Película 1', 'Película 2', 'Película 3', 'Película 4'],
    'Año': [2020, 2019, 2021, 2018]
})

# Dataframe en Spark
criticas = spark.createDataFrame([
    (1, 'Critico 1', 4.5),
    (2, 'Critico 2', 3.8),
    (3, 'Critico 1', 4.2),
    (4, 'Critico 3', 4.7),
    (4, 'Critico 6', 3.9) # DATO AGREGADO!
], ['PeliculaID', 'Critico', 'Puntuacion'])

# Convertir df "datos_peliculas" de Pandas a Spark
datos_peliculas = spark.createDataFrame(datos_peliculas)

# Puntuación promedio por película
criticas_avg = criticas.groupBy('PeliculaID').agg(F.avg('Puntuacion').alias('PuntuacionPromedio'))

# Unir dataframes
result = datos_peliculas.join(criticas_avg, datos_peliculas.ID == criticas_avg.PeliculaID, 'inner')
result.select('Título', 'Año', 'PuntuacionPromedio').show()