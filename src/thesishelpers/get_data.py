import requests
import pandas as pd
import geopandas as gpd
from pyproj import CRS
from shapely.geometry import Point
# TODO: Add temporal resolution column to datasets?


def request_netatmo(
    client_ID: str,
    client_secret: str,
    pw: str,
    username: str,
    bounding_gdf: gpd.GeoDataFrame,
    place: str,
    output_crs: str = "EPSG:32632",
):
    """Gets precipitation stations through netatmos api

    Arguments:
        client_ID {str} --
        client_secret {str} --
        pw {str} -- password
        username {str} -- username
        bounding_gdf {gpd.GeoDataFrame} -- gdf where the first line has a
                                           column "epsg:4326"
        with a tuple (x_SW, y_SW, x_NE, y_NE)

    Keyword Arguments:
        output_crs {str} -- Output coordinate reference system (default: {'EPSG:32632'})

    Raises:
        Exception: [description]
        Exception: [description]

    Returns:
        gpd.GeoDataFrame -- Georeferenced dataframe of the values
    """

    # Authentication
    auth_params = {
        "client_id": client_ID,
        "client_secret": client_secret,
        "grant_type": "password",
        "username": username,
        "password": pw,
        "scope": "read_station",
    }
    auth_endpoint = "https://api.netatmo.com/oauth2/token"
    auth = requests.post(auth_endpoint, auth_params)
    auth_json = auth.json()
    if auth.status_code != 200:
        raise Exception(f"token request failed, response: {auth.text}")
    else:
        print(f"token request succeded.")
    token = auth_json["access_token"]

    # Create bounding box:
    if place not in bounding_gdf.place.values:
        raise ValueError(f"{place} not in {bounding_gdf.place.values}")
    names = ("lon_sw", "lat_sw", "lon_ne", "lat_ne")
    coords = bounding_gdf[bounding_gdf.place == place].to_crs('epsg:4326').geometry[0].bounds
    bbox = dict(zip(names, coords))

    # Get data
    endpoint = "https://api.netatmo.com/api/getpublicdata"
    parameters = {
        **bbox,
        "required_data": "rain",
    }
    r = requests.get(endpoint, parameters, headers={"Authorization": "Bearer " + token})
    json = r.json()
    if r.status_code != 200:
        raise Exception(
            f"data request returned error code {r.status_code}.\
              {json['error']['message']}"
        )
    else:
        print("data resquest succeded")

    # Initial processing
    df_raw = pd.DataFrame.from_dict(json)
    df = pd.DataFrame.from_records(df_raw.body)
    df = df.drop(["measures", "modules", "module_types"], axis=1)

    df["lon"] = df.place.apply(lambda x: x["location"][0])
    df["lat"] = df.place.apply(lambda x: x["location"][1])
    df["masl"] = df.place.apply(lambda x: x["altitude"])
    df["country"] = df.place.apply(lambda x: x["country"])
    df = df.drop(["place"], axis=1)
    df = df[df.country == "NO"]  # filters by stations in norway
    df = df.rename(columns={"_id": "id"})

    # Generate GeoDataFrame
    gdf = gpd.GeoDataFrame(df)
    gdf["geometry"] = gdf.apply(lambda x: Point(x["lon"], x["lat"]), axis=1)
    gdf = gdf.drop(["lon", "lat"], axis=1)

    # Assign, then change the crs
    gdf.crs = CRS.from_epsg(4326)
    gdf = gdf.to_crs(output_crs)

    gdf["source"] = "NETATMO"

    return gdf


def request_frost(
    client_ID: str,
    client_secret: str,
    resolution: str,
    bounding_gdf: gpd.GeoDataFrame = None,
    place: str=None,
    output_crs: str = "EPSG:32632",
):

    elements = {
        "monthly": r"sum(precipitation_amount P1M)",
        "daily": r"sum(precipitation_amount P1D)",
        "hourly": r"sum(precipitation_amount PT1H)",
        "10_min": r"sum(precipitation_amount PT10M)",
    }
    endpoint = "https://frost.met.no/sources/v0.jsonld"
    parameters = {
        "types": "SensorSystem",
        "country": "No",
        "fields": "id,geometry,masl,stationholders,wmoid",
    }
    if resolution in elements:
        parameters["elements"] = elements[resolution]
    else:
        raise ValueError(f"resolution argument must be one of {[k for k in elements]}.")

    # Get data
    r = requests.get(endpoint, parameters, auth=(client_ID, client_secret))
    json = r.json()
    if r.status_code != 200:
        raise Exception(
            f"request returned error code {r.status_code}.\
              {json['error']['message']}: {json['error']['reason']}"
        )
    else:
        print("resquest succeded")

    data = json["data"]
    df = pd.DataFrame.from_dict(data)
    df["lon"] = df.geometry.apply(lambda x: x["coordinates"][0])
    df["lat"] = df.geometry.apply(lambda x: x["coordinates"][1])
    df = df.drop(["geometry"], axis=1)

    gdf = gpd.GeoDataFrame(df)
    gdf["geometry"] = gdf.apply(lambda x: Point(x["lon"], x["lat"]), axis=1)
    gdf = gdf.drop(["lon", "lat"], axis=1)

    # Assign, the change CRS
    gdf.crs = CRS.from_epsg(4326)
    gdf = gdf.to_crs(output_crs)

    # Gets rid of stationholders as list
    def cat_to_str(li):
        return ",".join(li)

    gdf["stationHolders"] = gdf["stationHolders"].apply(cat_to_str)
    gdf = gdf.rename(columns={"stationHolders": "owners"})

    # filter by research area
    if bounding_gdf is not None and place:
        bound_polygon = bounding_gdf[bounding_gdf.place == place].geometry[0]
        gdf = gdf[gdf.geometry.within(bound_polygon)]

    gdf["source"] = "MET"
    return gdf


def load_CML(path: str, bounding_gdf: gpd.GeoDataFrame = None, place=None):
    gdf = gpd.GeoDataFrame.from_file(path)
    if bounding_gdf is not None and place:
        bound_polygon = bounding_gdf[bounding_gdf.place == place].geometry[0]
        gdf = gdf[gdf.geometry.within(bound_polygon)]

    gdf["source"] = "CML"
    return gdf
