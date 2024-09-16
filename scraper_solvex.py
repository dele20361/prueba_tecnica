# Paola De León
# Prueba técnica Solvex
# Ejercicio 4: Ejercicio de Web Scraping con Requests

import requests
from bs4 import BeautifulSoup

# URL de la página de CoinMarketCap
url = 'https://coinmarketcap.com/currencies/bitcoin/'

# Realiza una solicitud GET para obtener el contenido de la página
response = requests.get(url)

# Verifica si la solicitud fue exitosa (código de respuesta 200)
if response.status_code == 200:
    # Analiza el contenido de la página web con BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    # Encuentra el elemento que contiene el precio actual del Bitcoin
    # Pista: Inspecciona la página web para identificar el elemento adecuado
    # Extrae el precio del elemento y almacénalo en una variable
    price = soup.find(attrs={'class':'sc-65e7f566-0 clvjgF base-text'})
    # Imprime el precio en la consola
    if price:
        price = price.text
        print(f'Precio: {price}')
    else:
        print('No se pudo encontrar el elemento.')
else:
    print(f'Error al hacer la solicitud. Código de respuesta: {response.status_code}')