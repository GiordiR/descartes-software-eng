import validators
from earthquakes.usgs_api import build_api_url
from datetime import datetime


def test_build_url_api():
    """
        Test earthquakes api url
    """
    url = build_api_url(35.025, 25.763, 200, 4.5, datetime(year=2000, month=10, day=21), datetime(year=2021, month=10, day=21))
        
    assert validators.url(url) == True