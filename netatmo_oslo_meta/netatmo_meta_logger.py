import requests
import pandas as pd
import geopandas as gpd
from pyproj import CRS
from shapely.geometry import Point
from datetime import datetime
import os
import yagmail
import schedule
import time
import traceback

## netatmo auth
netatmo_ID = '5e5665a141a1132dc14b05d8'
netatmo_secret = 'IDtKbcIsbj85bpyNNq0qaXqEC'
netatmo_pw = "3YSv4CPRSW$Z^sY"
netatmo_user = "dapr@nmbu.no"

## Email auth
email_user = "netatmoprecipitation.logger@gmail.com"
email_pw = "%*4jcd4GAB9bppu"

## Paths
grid_path = "./grid.geojson"
metadata_path = "./meta_oslo.geojson"

station_counts = [0]

def request_netatmo(
    client_ID: str,
    client_secret: str,
    pw: str,
    username: str,
    bounding_gdf: gpd.GeoDataFrame,
    areal_buffer: float,
    output_crs: str = "EPSG:32632",
    verbose=False
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
        if verbose:
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
    if verbose:
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
        if verbose:
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
    gdf["resolution"] = 60
    if bounding_gdf is not None:
        bound_polygon = bounding_gdf.geometry.iloc[0]
        clipped_gdf = gdf[gdf.geometry.within(bound_polygon)]
        removed_stations = len(gdf) - len(clipped_gdf)
        if verbose:
            print(f"{removed_stations} statinos exceeded study area and were removed.")

    return clipped_gdf


def send_email(message, subject, attachment_path=None, to="dapr@nmbu.no"):
    with yagmail.SMTP(user=email_user, password=email_pw) as connection:
        connection.send(
            to=to, subject=subject, contents=message, attachments=attachment_path
        )

def get_all_meta():
    meta = gpd.GeoDataFrame.from_file(metadata_path)
    grid = gpd.GeoDataFrame.from_file(grid_path)
    for i in range(len(grid)):
        meta_update = request_netatmo(
            client_ID=netatmo_ID,
            client_secret=netatmo_secret,
            pw=netatmo_pw,
            username=netatmo_user,
            bounding_gdf=grid.iloc[[i]],
            areal_buffer=0,
        )
        meta = pd.concat([meta, meta_update]).drop_duplicates(
            subset=["id"], keep="first"
        )
    meta.to_file(metadata_path, driver='GeoJSON')
    station_counts.append(len(meta))

def try_it():
    subject = ""
    contents = ""
    try:
        get_all_meta()
    except Exception as e:
        subject = "error occured"
        contents = str(e)
        traceback.print_exc()
        send_email(message=contents, subject=subject)

def send_report():
    send_email(
        message=str(station_counts),
        subject=f"currently {station_counts[-1]} stations."
    )



if __name__ == "__main__":
    try_it()
    pass
    schedule.every(14).minutes.do(try_it)
    schedule.every(3).hours.do(send_report)

    while True:
        schedule.run_pending()
        time.sleep(1)


