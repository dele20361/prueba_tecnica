# Paola De León
# Prueba técnica Solvex
# Ejercicio 1: Manipulación de datos con Pandas y conjunto de datos de COVID-19

import pandas as pd

# Crear dataframe
deaths = pd.read_csv('COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
confirmed = pd.read_csv('COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')

# Eliminar columnas
deaths.drop(['Lat', 'Long'], inplace=True, axis=1)
confirmed.drop(['Lat', 'Long'], inplace=True, axis=1)

# Agrupar información por país
grouped_deaths = deaths.groupby('Country/Region').sum(numeric_only=True)
grouped_confirmed = confirmed.groupby('Country/Region').sum(numeric_only=True)

# Agrupar país, cantidad de fallecimientos y casos confirmados.
country_stats = pd.DataFrame({
    'Death': grouped_deaths.sum(axis=1),
    'Confirmed': grouped_confirmed.sum(axis=1)
}).reset_index()


# ---------------------------------------------------- Ejercicio 1.C --------------------------------------------------- #
# Calcula el promedio de casos confirmados por día en un país específico
print("\n\n • Inciso C: Calcula el promedio de casos confirmados por día en un país específico")
country = 'Australia'

country_info = country_stats[country_stats['Country/Region'] == country]
cant_dates = (len(confirmed.columns) - 2)
mean_confirmed = country_info['Confirmed'].sum() / cant_dates

print(f"\nPromedio de casos Confirmed en {country}: {mean_confirmed}")


# ---------------------------------------------------- Ejercicio 1.D --------------------------------------------------- #
# Ejercicio 1.D: Encuentra los 10 países con la tasa de mortalidad más alta (número de muertes / número de casos confirmados) hasta la fecha
print("\n\n • Inciso D: Encuentra los 10 países con la tasa de mortalidad más alta (número de muertes / número de casos confirmados) hasta la fecha")

country_stats['Tasa Mortalidad'] = country_stats['Death'] / country_stats['Confirmed']
top10 = country_stats.sort_values(by='Tasa Mortalidad', ascending=False).head(10)

print("\n", top10)