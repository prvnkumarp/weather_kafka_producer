import requests


class OWeather:

    @staticmethod
    def get_open_meteo_weather(params, url="https://api.open-meteo.com/v1/forecast"):
        response = requests.get(url, params)
        return response.json()

    @staticmethod
    def get_geocoding(params, url="https://geocoding-api.open-meteo.com/v1/search"):
        response = requests.get(url, params)
        return response.json()["results"][0]
