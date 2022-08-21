import json
from dataclasses import dataclass
from typing import Dict, Optional

import requests
import os
from dotenv import load_dotenv

@dataclass
class OpenWeatherMap:
    api_key: str
    base_url: str

    def __init__(self):
        load_dotenv()
        self.api_key            = os.getenv('KEY')
        self.base_url  = 'https://api.openweathermap.org/data/2.5'
        self.geo_url   = 'http://api.openweathermap.org/geo/1.0'

    def build_url(self, city: json) -> str:
        # https://api.openweathermap.org/data/2.5/weather?lat=33.44&lon=-94.04&exclude=hourly,daily&appid={API key}
        static_params = 'units=metric&exclude=current,minutely,hourly,alerts&lang=de'
        url = f'{self.base_url}/weather?lat={city["latitude"]}&lon={city["longitude"]}&{static_params}&appid={self.api_key}'
        return url

    def get_coordinates(self, city: str, country_code: str = None):
        # http://api.openweathermap.org/geo/1.0/direct?q={city name},{state code},{country code}&limit={limit}&appid={API key}
        if country_code is not None:
            city = f'{city},{country_code}'
        url = f'{self.geo_url}/direct?q={city}&appid={self.api_key}'
        try:
            api_response = requests.get(url, verify=True, timeout=10)
            if api_response.ok:
                res = json.loads(api_response.content)
                return res[0]
        except Exception as e:
            print(f'    ! error while loading url {url}: {e}')
        return None

    def get_forecast(self, city: json) -> Optional[Dict]:
        url = self.build_url(city)
        try:
            api_response = requests.get(url, verify=True, timeout=100)
            if api_response.ok:
                return json.loads(api_response.content)
        except Exception as e:
            print(f'    ! error while loading url {url}: {e}')
        return None
