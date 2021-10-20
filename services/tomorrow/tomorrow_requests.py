#!/usr/bin/env python
from dotenv import load_dotenv
import os
import requests
from datetime import datetime
from services.tomorrow.helpers import TIMEZONES_DICT, TIME_STEPS_LIST

END_POINT = 'https://api.tomorrow.io/v4/timelines'

# Getting environment variables
load_dotenv()

API_KEY = os.environ['TOMORROW_API_KEY']


# Class with all available api dimensions
class options_cls:
    def __init__(self, options:list):
        for option in options:
            class_name = option.upper().replace("/","_")
            class_name = "_" + class_name if class_name[0].isnumeric() else class_name
            setattr(self, class_name, option)


class dimension:
    def __init__(self, name:str, dimension_type, option_list:list=None, notes:str=None):
        self.name = name
        self.options = options_cls(option_list)
        self.dimension_type = dimension_type
        self.notes = notes


    def set_value(self, value):
        if not isinstance(value, self.dimension_type):
            raise TypeError(f"{value} is not type {self.dimension_type}")
        else:
            self.value = value


class dimensions:
    class fields:
        TEMPURATURE = dimension('temperature', str)
        TIMESTEPS = dimension('timesteps', str)
        UNITS = dimension('units', str)
        API_KEY = dimension('apikey', str)

    class date_time:
        START_DATE = dimension("startTime", datetime)
        END_DATE = dimension("endTime", datetime)
        TIMEZONE = dimension("timezone", str, TIMEZONES_DICT.keys())
        TIME_STEPS = dimension("timesteps", str, TIME_STEPS_LIST)

    class location:
        LOCATION = dimension("location", set)

DEFAULT_TIMEZONE = dimensions.date_time.TIMEZONE.options.AMERICA_CHICAGO
DEFAULT_TIME_STEPS = dimensions.date_time.TIME_STEPS.options._1H


# Making Api Requests
class api_requests:
    def __init__(self,api_key=API_KEY):
        self.api_key = api_key


    def make_request(self, location:dimensions.location.LOCATION, fields:dimensions.fields, 
                    start_date:dimensions.date_time.START_DATE, end_date:dimensions.date_time.END_DATE, 
                    timezone:dimensions.date_time.TIMEZONE=DEFAULT_TIMEZONE, time_steps:dimensions.date_time.TIME_STEPS=DEFAULT_TIME_STEPS):

        headers = {"Content-Type": "application/json; charset=utf-8", "Connection":"keep-alive"}
        
        if not isinstance(location, dimensions.location.LOCATION):
            value = location
            location = dimensions.location.LOCATION
            location.set_value(value)

        if not isinstance(start_date, dimensions.date_time.START_DATE):
            value = start_date
            start_date = dimensions.date_time.START_DATE
            start_date.set_value(value)

        if not isinstance(end_date, dimensions.date_time.END_DATE):
            value = end_date
            start_date = dimensions.date_time.END_DATE
            start_date.set_value(value)

        if not isinstance(timezone, dimensions.date_time.TIMEZONE):
            value = timezone
            start_date = dimensions.date_time.TIMEZONE
            start_date.set_value(value)

        if not isinstance(time_steps, dimensions.date_time.TIME_STEPS):
            value = time_steps
            start_date = dimensions.date_time.TIME_STEPS
            start_date.set_value(value)

        for field in fields:
            pass

            #TODO Stopped here

        param_cls = [location, start_date, end_date, timezone, time_steps]
        

        params = {x.name: x.value for x in param_cls}
        params["fields"] = [x.name for x in fields]
        params[dimensions.fields.API_KEY.name] = self.api_key

        req = requests.get(END_POINT, params, headers=headers)
        return req.json()
        