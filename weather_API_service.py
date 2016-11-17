import urllib2
import json
import models
import settings

from dateutil import parser


db_tool = models.DB_tool()


def copy_API_data_to_db(site):
    print site
    data, updated = retrieve_data(site)
    weather_data = {}
    weather_data['site_id'] = site.id
    weather_data['updated'] = updated

    persist_weather(data, weather_data)


def retrieve_data(site):
    response = urllib2.urlopen(settings.URL_WEATHER
                               + "lat=" + str(site.latitude)
                               + "&lon="
                               + str(site.longitude))
    data = json.loads(response.read())
    data = data['LocationWeather']
    updated = parser.parse(data['latestobservation']['latests'][0]['dateTime'])
    return data, updated


def init_missing_values(data):
    features = set(['ne', 'ww', 'rrr', 'tt', 'tx', 'tn', 'prrr'])
    data_keys = set(data.keys())
    missing_keys = features - data_keys
    for missing_key in (missing_keys):
        data[missing_key] = -9999
    return data


def persist_weather(data, weather_data):
    for d in data['observation']['hours']:
        weather_data['period'] = 'hour'
        weather_data_hour = add_observation(d, weather_data)
        weather_data_hour['data_type'] = 'observation'
        weather_data_db = models.WeatherData(**weather_data_hour)
        db_tool.store_weather_data(weather_data_db)

    for d in data['observation']['days']:
        weather_data['period'] = 'day'
        weather_data_day = add_observation(d, weather_data)
        weather_data['data_type'] = 'observation'
        weather_data_db = models.WeatherData(**weather_data_hour)
        db_tool.store_weather_data(weather_data_db)

    for d in data['forecast']['days']:
        weather_data['period'] = 'day'
        weather_data_hour = add_observation(d, weather_data)
        weather_data_hour['data_type'] = 'forecast'
        weather_data_db = models.WeatherData(**weather_data_hour)
        db_tool.store_weather_data(weather_data_db)


def add_observation(obs_data, weather_data):

    result = init_missing_values(obs_data)

    if weather_data['period'] == 'hour':
        obs_data['dateTime'] = parser.parse(obs_data['dateTime'])
    elif weather_data['period'] == 'day':
        obs_data['dateTime'] = parser.parse(obs_data['date'])
        obs_data.pop('date', None)

    result.update(obs_data)

    for k, v in result.iteritems():
        weather_data[k] = v

    return weather_data
