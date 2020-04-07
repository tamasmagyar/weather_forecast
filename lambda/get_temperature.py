import json
import logging
import os
from dataclasses import dataclass

import requests
import time

import boto3
from pyowm import OWM
from pyowm.weatherapi25.observation import Observation
from pyowm.weatherapi25.weather import Weather

logger = logging.getLogger()
logger.setLevel(logging.INFO)

COORDS = (46.703321, 19.851507)
SENDER_EMAIL = os.environ.get('SENDER_EMAIL')
RECEIVER_EMAIL = os.environ.get('RECEIVER_EMAIL')
OWM_API_KEY = os.environ.get('OWM_API_KEY')
WEATHERBIT_API_KEY = os.environ.get('WEATHERBIT_API_KEY')
REGION = "eu-central-1"
logging.info(f'{SENDER_EMAIL=}, {RECEIVER_EMAIL=}, {OWM_API_KEY=}, {WEATHERBIT_API_KEY=}')

api_param = f'&key={WEATHERBIT_API_KEY}'
location_param = f'?lat={COORDS[0]}&lon={COORDS[1]}'
base_api_call = 'https://api.weatherbit.io/v2.0/forecast/'
current_api_call = f'{base_api_call}current{location_param}'
daily_api_call = f'{base_api_call}daily{location_param}&days=1{api_param}'
hourly_api_call = f'{base_api_call}hourly{location_param}{api_param}&hours=12'


@dataclass
class DailyForecast:
    def __init__(self, max_temp, pop, precip):
        self.max_temp = max_temp
        self.pop = pop
        self.precip = precip


def get_current_temperature() -> dict:
    owm = OWM(OWM_API_KEY)
    observation: Observation = owm.weather_at_coords(*COORDS)
    weather: Weather = observation.get_weather()
    temperature_dict: dict = weather.get_temperature('celsius')
    current_temperature = temperature_dict['temp']
    return current_temperature


def get_daily_data() -> dict:
    api_response = requests.get(daily_api_call)
    content = json.loads(api_response.content)
    return content['data'][0]


def get_hourly_data() -> list:
    api_response = requests.get(hourly_api_call)
    content = json.loads(api_response.content)
    return content['data']


def get_hourly_temperatures() -> list:
    hourly_weather = get_hourly_data()
    return [hour['temp'] for hour in hourly_weather]


def get_daily_forecast() -> DailyForecast:
    weather_data = get_daily_data()
    daily_forecast = DailyForecast(max_temp=float(weather_data['high_temp']),
                                   pop=float(weather_data['pop']),
                                   precip=float(weather_data['precip']))

    return daily_forecast


def pair_hourly_temperature() -> list:
    current_hour = int(time.strftime("%H"))
    hourly_temps = []
    for hour, temperature in zip(range(11), get_hourly_temperatures()):
        next_hour = current_hour + hour
        if next_hour > 23:
            next_hour -= 24
        hourly_temps.append((next_hour, temperature))
    return hourly_temps


def generate_email_body_text(daily_forecast: DailyForecast, hourly_data, current_temp):
    body_text = f"Current temperature is {current_temp}°C\r\n"
    body_text += f"Max temperature today is {daily_forecast.max_temp}°C\r\n"

    if daily_forecast.pop or daily_forecast.precip:
        body_text += f'Rain pop: {daily_forecast.pop}%, Rain precip: {daily_forecast.precip}mm \n'
    else:
        body_text += 'Rain is not expected for today. \n'

    if hourly_data:
        body_text += f'Temperature for the next hours:\n'
        for i in hourly_data:
            body_text += f'\t \t{i[0]}:00 - {i[1]}°C\n'

    return body_text


def send_email(email_body: str, email: str = RECEIVER_EMAIL):
    client = boto3.client('ses', region_name=REGION)
    response = client.send_email(
        Source=SENDER_EMAIL,
        Destination={
            'ToAddresses': [
                email,
            ],
        },
        Message={
            'Body': {
                'Text': {
                    'Charset': "UTF-8",
                    'Data': email_body,
                },
            },
            'Subject': {
                'Charset': "UTF-8",
                'Data': 'weather forecast',
            },
        },
    )

    logging.info(f'{response=}')


def get_secrets(env_variables: list) -> list:
    secret_arn = os.environ.get('API_KEYS')
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=REGION
    )

    secret_response = client.get_secret_value(SecretId=secret_arn)
    secret_string = json.loads(secret_response['SecretString'])
    return [secret_string[secret] for secret in env_variables]


def lambda_handler(event, context):
    email_body = generate_email_body_text(daily_forecast=get_daily_forecast(),
                                          hourly_data=pair_hourly_temperature(),
                                          current_temp=get_current_temperature())
    send_email(email_body=email_body)
