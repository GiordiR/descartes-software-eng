import os, io, asyncio, aiohttp
import pandas as pd
from urllib.request import urlopen
from urllib.parse import urlunsplit, urlencode
from datetime import datetime, timedelta

def build_api_url(latitude, longitude, radius, minimum_magnitude, start_date, end_date):
    """Build api url for data request

    Args:
        latitude (float): Earthquake latitude
        longitude (float): Earthquake longitude
        radius (float): Search radius
        minimum_magnitude (float): Minimum magnitude
        start_date (datetime.datetime): Initial date
        end_date (datetime.datetime): Final date

    Returns:
        str: Url for the specific query
    """
    SCHEME = os.environ.get("API_SCHEME", "https")
    NETLOC = os.environ.get("API_NETLOC", "earthquake.usgs.gov")
    PATH   = '/fdsnws/event/1/query'
    QUERY  = urlencode(dict(format="csv", starttime=start_date, endtime=end_date, 
                                   latitude=latitude, longitude=longitude, maxradiuskm=radius,
                                   minmagnitude=minimum_magnitude))
    
    return urlunsplit((SCHEME, NETLOC, PATH, QUERY, ""))


def get_earthquake_data(latitude, longitude, radius, minimum_magnitude, end_date):
    """Retrieve the earthquake data of the area of interest for the past 200 years

    Args:
        latitude (float): Earthquake latitude
        longitude (float): Earthquake longitude
        radius (float): Search radius
        minimum_magnitude (float): Minimum magnitude
        end_date (datetime.datetime): Final date

    Returns:
        pandas.dataframe: Earthquake dataframe for the last 200 year
    """
    start_date = end_date - timedelta(days=73000)
    
    html = build_api_url(latitude, longitude, radius, minimum_magnitude, start_date, end_date)
    
    with urlopen(html) as response:
        return pd.read_csv(response)

async def fetch_data(session, latitude, longitude, radius, minimum_magnitude, start_date, end_date):
    """Get eartquakes data for one location in asyncronous way

    Args:
        session (aiohhtp.ClientSession): Client session
        latitude (float): Earthquake latitude
        longitude (float): Earthquake longitude
        radius (float): Search radius
        minimum_magnitude (float): Minimum magnitude
        start_date (datetime.datetime): Initial date
        end_date (datetime.datetime): Final date

    Returns:
        str: Url for the specific query
    """
    api_url = build_api_url(latitude, longitude, radius, minimum_magnitude, start_date, end_date)
    async with session.get(api_url) as response:
        return await response.text()   
    
async def get_earthquake_data_for_multiple_locations(assets, radius, minimum_magnitude, end_date):
    """Retrieve the earthquake data of the area of interest for the past 200 years for multiple locations

    Args:
        assets (list): List of earthquake locations [(latitude1, longitude1),...]
        radius (float): Search radius
        minimum_magnitude (float): Minimum magnitude
        end_date (datetime.datetime): Final date

    Returns:
        pandas.dataframe: Earthquake dataframe for multiple requested locations
    """
    start_date = end_date - timedelta(days=73000)
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for latitude, longitude in assets:
            task = fetch_data(session, latitude, longitude, radius, minimum_magnitude, start_date, end_date)
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        
    earthquake_data = [pd.read_csv(io.StringIO(response)) for response in responses]
    earthquake_data = pd.concat(earthquake_data, ignore_index=True)

    return earthquake_data