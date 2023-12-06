import math

EARTH_RADIUS = 6378

TIME_COLUMN      = "time"
PAYOUT_COLUMN    = "payout"
MAGNITUDE_COLUMN = "mag"
DISTANCE_COLUMN  = "distance"
LATITUDE_COLUMN  = "latitude"
LONGITUDE_COLUMN = "longitude"


def get_haversine_distance(earthquakes_latitudes, earthquakes_longitudes, client_latitude, client_longitude):
    """ Calculate distance between two location using haversine formula

    Args:
        earthquakes_latitudes (float):earthquakes latitudes
        earthquakes_longitudes (float): earthquakes longitudes
        client_latitude (float): client latitude
        client_longitude (float): client longitude

    Returns:
        float: return distance between client and earthquake position 
    """
    distances = []
    
    for i in range(len(earthquakes_latitudes)):
        
        # Convert latitude and longitude from degrees to radians
        client_latitude_rad = math.radians(client_latitude)
        client_longitude_rad = math.radians(client_longitude)
        earthquake_latitude_rad = math.radians(earthquakes_latitudes[i])
        earthquake_longitude_rad = math.radians(earthquakes_longitudes[i])

        # Calculate differences
        delta_latitude = abs(client_latitude_rad - earthquake_latitude_rad)
        delta_longitude = abs(client_longitude_rad - earthquake_longitude_rad)

        # Use explicit distance function
        h = math.sin(delta_latitude / 2.) ** 2 + math.cos(client_latitude_rad) * math.cos(earthquake_latitude_rad) * math.sin(delta_longitude / 2.) ** 2
        h = math.sqrt(h)

        distance = 2 * EARTH_RADIUS * math.asin(h)

        distances.append(distance)
    
    return distances


def compute_payouts(earthquake_data, payout_structure):
    """Calculate payouts based on payout structure

    Args:
        earthquake_data (pandas.dataframe): Pandas dataframe to be evaluate for the payout
        payout_structure (pandas.dataframe): Pandas dataframe containing the payout structure

    Returns:
        pandas.Series: Pandas series containing the payout associated to every year in the earthquake database
    """
    earthquake_data[TIME_COLUMN] = earthquake_data[TIME_COLUMN].str[:4]
    
    calculate_payout = lambda row: (
        payout_structure[PAYOUT_COLUMN][0] if row[DISTANCE_COLUMN] < payout_structure[DISTANCE_COLUMN][0] and row[MAGNITUDE_COLUMN] > payout_structure[MAGNITUDE_COLUMN][0]
        else (payout_structure[PAYOUT_COLUMN][1] if row[DISTANCE_COLUMN] < payout_structure[DISTANCE_COLUMN][1] and row[MAGNITUDE_COLUMN] > payout_structure[MAGNITUDE_COLUMN][1]
            else (payout_structure[PAYOUT_COLUMN][2] if row[DISTANCE_COLUMN] < payout_structure[DISTANCE_COLUMN][2] and row[MAGNITUDE_COLUMN] > payout_structure[MAGNITUDE_COLUMN][2] else 0))
    )
    
    earthquake_data[PAYOUT_COLUMN] = earthquake_data.apply(calculate_payout, axis=1)
    
    return earthquake_data.groupby(TIME_COLUMN)[PAYOUT_COLUMN].max()


def compute_burning_cost(payouts, start_year, end_year):
    """Calculate the burning cost in a selected time range

    Args:
        payouts (pandas.dataframe): _description_
        start_year (int): Initial year considered
        end_year (int): Final year considered

    Returns:
        float: Average burning cost 
    """
    ranged_payouts = payouts.loc[str(start_year):str(end_year)]
    
    return ranged_payouts.mean()
