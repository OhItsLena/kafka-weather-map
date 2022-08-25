import json
import os

from OpenWeatherMap import OpenWeatherMap

# This file is used to query lat/long information for city from the OpenWeatherMap Geocoding API
# https://openweathermap.org/api/geocoding-api

openWeatherMap = OpenWeatherMap()  # using our OpenWeatherMap class

# list of cities we want to find lat/long for
# city name + country code to facilitate getting the right city
cities = [
    ('Bregenz', 'AT'),
    ('Wien', 'AT'),
    ('Paris', 'FR'),
    ('London', 'GB'),
    ('Prag', 'CZ'),
    ('Florenz', 'IT'),
    ('Kopenhagen', 'DK'),
    ('Aachen', 'DE'),
    ('Berlin', 'DE'),
    ('Bonn', 'DE'),
    ('Dresden', 'DE'),
    ('Frankfurt am Main', 'DE'),
    ('Freiburg', 'DE'),
    ('Kiel', 'DE'),
    ('Köln', 'DE'),
    ('Leipzig', 'DE'),
    ('Lübeck', 'DE'),
    ('Mannheim', 'DE'),
    ('München', 'DE'),
    ('Münster', 'DE'),
    ('Ravensburg', 'DE'),
    ('Rostock', 'DE'),
    ('Stuttgart', 'DE')
]

# save json to disk as locations.json in current directory
def save_locations(data: json) -> bool:
    filename = 'locations.json'
    temp_filename = f'{filename}.tmp'
    try:
        with open(temp_filename, mode='w') as f:
            json.dump(data, f, indent=4)
    except TypeError as te:
        print(f'        !!! could not write file: {te}')
        return False
    os.rename(temp_filename, filename)
    return True

# use OpenWeatherMap class to get lat/long for every city in the list
def get_coordinates() -> None:
    locations = {}
    for city in cities:
        data = openWeatherMap.get_coordinates(city[0], country_code=city[1])
        locations[city[0]] = {}
        locations[city[0]]['latitude'] = data["lat"]
        locations[city[0]]['longitude'] = data["lon"]
    print(json.dumps(locations, indent=4))
    save_locations(locations)

# save lat/long for cities to locations.json when executing the .py file
get_coordinates()
