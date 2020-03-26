import requests
import pandas as pd
import geopandas as gpd
from pyproj import CRS
from shapely.geometry import Point
from collections import OrderedDict

# TODO: Add temporal resolution column to datasets?


def request_netatmo(
    client_ID: str,
    client_secret: str,
    pw: str,
    username: str,
    bounding_gdf: gpd.GeoDataFrame,
    areal_buffer: float,
    output_crs: str = "EPSG:32632",
):
    # TODO: update docstring for new inputs
    """Gets precipitation stations through netatmos api

    Arguments:
        client_ID {str} --
        client_secret {str} --
        pw {str} -- password
        username {str} -- username
        bounding_gdf {gpd.GeoDataFrame} -- gdf where first line has bounding geometry

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

    names = ("lon_sw", "lat_sw", "lon_ne", "lat_ne")
    coords = (
        bounding_gdf
        .buffer(areal_buffer)
        .to_crs("epsg:4326")
        .geometry.iloc[0]
        .bounds
    )
    bbox = dict(zip(names, coords))
    print(f"after {areal_buffer}m buffer, requested bounding box was: {bbox}")

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
    gdf['owner'] = "PRIVATE"
    gdf["resolution"] = "hourly"
    if bounding_gdf is not None:
        bound_polygon = bounding_gdf.geometry.iloc[0]
        clipped_gdf = gdf[gdf.geometry.within(bound_polygon)]
        removed_stations = len(gdf) - len(clipped_gdf)
        print(f"{removed_stations} statinos exceeded study area and were removed.")

    return clipped_gdf

def find_primary_owner(owners:str, owner_importance: OrderedDict=None, default_owner:str="OTHER") -> str:
    """Assigns the primary owner by the first occuring defined by the dict owner_importance.
       Otherwise it assignes default value.
    
    Arguments:
        owners {str} -- string of all station owners
    
    Keyword Arguments:
        owner_importance {OrderedDict[str,str]} -- Ordered dictionary (by importance) of owners (default: {None})
        default_owner {str} -- what to assign primary owner if none of the owners occur in owner importance (default: {"private"})
    
    Returns:
        str -- primary owner
    """
    if owner_importance is None:
        owner_importance = OrderedDict({
            "MET.NO": "MET.NO",
            "NVE": "NVE",
            "STATENS VEGVESEN": "SVV",
            "NIBIO": "NIBIO",
            "BANE NOR": "BANE NOR",
            "KOMMUNE": "MUNICIPALITY",
            "ENERGI": "ENERGY",
            "KRAFT": "ENERGY",
            "STATNETT": "ENERGY",
            "": default_owner
        })
    for key, value in owner_importance.items():
        if key in owners:
            # TODO: info about the owners grouped to other
            return value


def request_frost(
    client_ID: str,
    client_secret: str,
    resolution: str = "all",
    bounding_gdf: gpd.GeoDataFrame = None,
    output_crs: str = "EPSG:32632",
):

    elements = OrderedDict({
        "monthly": r"sum(precipitation_amount P1M)",
        "daily": r"sum(precipitation_amount P1D)",
        "hourly": r"sum(precipitation_amount PT1H)",
        "10_min": r"sum(precipitation_amount PT10M)",
        "1_min": r"sum(precipitation_amount PT1M)"
    })
    endpoint = "https://frost.met.no/sources/v0.jsonld"
    parameters = {
        "types": "SensorSystem",
        "country": "No",
        "fields": "id,geometry,masl,stationholders,wmoid",
    }
    if resolution in elements:
        parameters["elements"] = elements[resolution]
    elif resolution == "all":
        # Wired subroutine, that should request all the resolutions and construct
        # a gdf with temporal resolution as column 
        dfs = []
        for res in elements:
            gdf = request_frost(
                client_ID=client_ID,
                client_secret=client_secret,
                resolution=res,
                bounding_gdf=bounding_gdf,
                output_crs=output_crs
            )
            gdf["resolution"] = res
            gdf = gdf.set_index("id")
            dfs.append(gdf)
        gdf = dfs.pop(0)  # Should be the monthly resolution dataset,
                          # This should also contain all with finer resolution.
        for other in dfs:
           gdf.update(other)
        # TODO: quality control, assert that lengths are correct etc.
        gdf = gdf.reset_index()
        return gdf
    else:
        raise ValueError(f"resolution argument must be one of {[k for k in elements]}, or \"all\"")

    # Get data
    r = requests.get(endpoint, parameters, auth=(client_ID, client_secret))
    json = r.json()
    if r.status_code != 200:
        raise Exception(
            f"request returned error code {r.status_code}.\
              {json['error']['message']}: {json['error']['reason']}"
        )
    else:
        print(f"resquest for {resolution} data succeded")

    # create df
    data = json["data"]
    df = pd.DataFrame.from_dict(data)

    # drop rows that don't include geometric information and sotre
    # coordinates
    df = df.dropna(subset=["geometry"])
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

    gdf["stationHolders"] = gdf["stationHolders"].apply(cat_to_str).apply(find_primary_owner)
    gdf = gdf.rename(columns={"stationHolders": "owner"})

    # filter by research area
    if bounding_gdf is not None:
        bound_polygon = bounding_gdf.geometry.iloc[0]
        gdf = gdf[gdf.geometry.within(bound_polygon)]
    
    gdf["source"] = "MET"
    return gdf



def load_CML(path: str, bounding_gdf: gpd.GeoDataFrame = None):
    gdf = gpd.GeoDataFrame.from_file(path)
    if bounding_gdf is not None:
        bound_polygon = bounding_gdf.geometry.iloc[0]
        gdf = gdf[gdf.geometry.within(bound_polygon)]
    gdf['owner'] = "TELIA"
    gdf["source"] = "CML"
    gdf["resolution"] = "hourly"
    return gdf
