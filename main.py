import json
import logging
import sys

from kafka.producer.Kafkaproducer import KafkaProducer
from requests_http.weather.weather import OWeather


def create_kafka_producer(topic: str):
    with open("./conf/kafka_producer.json") as producer_conf_file:
        kafka_producer_conf = json.load(producer_conf_file)
    return KafkaProducer(kafka_producer_conf, topic)


def cities_config():
    with open("./conf/cities_weather.json") as cities_config_file:
        config = json.load(cities_config_file)
        return config["cities"]


def main():
    cities_list = cities_config()
    logging.info(f"getting weather info for the cities list {','.join(cities_list)}")
    for city in cities_list:
        geo_param = {
            "name": city,
            "count": 1,
            "language": "en",
            "format": "json"
        }
        logging.info(f"getting geocoding for the city {city}")
        city_json_response = OWeather.get_geocoding(geo_param)
        weather_param = {
            "latitude": city_json_response['latitude'],
            "longitude": city_json_response['longitude'],
            "hourly": "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,"
                      "weather_code,pressure_msl,surface_pressure,visibility,wind_speed_10m,wind_direction_10m,"
                      "soil_temperature_0cm,soil_temperature_6cm,soil_moisture_0_to_1cm,soil_moisture_1_to_3cm",
            "past_days": 1,
            "forecast_days": 0
        }
        logging.info(f"getting weather data for the city {city}")
        weather_response = OWeather.get_open_meteo_weather(weather_param)
        total_response = city_json_response | weather_response
        weather_raw_kafka_producer = create_kafka_producer("weather_raw_history")
        weather_raw_kafka_producer.send_msg(json.dumps(total_response), key=city)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    main()
