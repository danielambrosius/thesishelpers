import requests
import pandas as pd
from datetime import datetime
import os
import yagmail
import schedule
import time


client_ID = "5e5665a141a1132dc14b05d8"
client_secret = "IDtKbcIsbj85bpyNNq0qaXqEC"
csv_path = "./_oslo.csv"
email_user = "netatmoprecipitation.logger@gmail.com"
email_pw = "%*4jcd4GAB9bppu"

oslo_bbox = {
    "lon_sw": 10.471082901350089,
    "lat_sw": 59.83419671828883,
    "lon_ne": 10.899505755119103,
    "lat_ne": 59.92014198248264,
}


def filter_json(json):
    # Filters JSON body to get timestamp and value of rain last hour in mm
    results = {}
    for station in json["body"]:
        for key, val in station["measures"].items():
            if "rain_60min" in val:
                rain_measures = val

        results[station["_id"]] = str(
            (rain_measures["rain_timeutc"], rain_measures["rain_60min"])
        )

    return results


def get_last_hour(bbox=oslo_bbox):

    # AUTHENTICATE
    auth_endpoint = "https://api.netatmo.com/oauth2/token"
    auth_params = {
        "client_id": client_ID,
        "client_secret": client_secret,
        "grant_type": "password",
        "username": "dapr@nmbu.no",
        "password": "3YSv4CPRSW$Z^sY",
        "scope": "read_station",
    }
    auth = requests.post(auth_endpoint, auth_params)
    auth_json = auth.json()
    if auth.status_code != 200:
        raise Exception(f"request failed, response: {auth.text}")
    token = auth_json["access_token"]

    # CREATE query
    endpoint = "https://api.netatmo.com/api/getpublicdata"
    parameters = {
        **bbox,
        "required_data": "rain",
    }

    # Get data
    r = requests.get(endpoint, parameters, headers={"Authorization": "Bearer " + token})
    json = r.json()
    if r.status_code != 200:
        raise Exception(
            f"Data request returned error code {r.status_code}. {json['error']['message']}"
        )

    # Create df
    results = filter_json(json)
    df = pd.DataFrame(columns=results.keys())
    df.loc[datetime.utcnow()] = results
    return df


def append_to_csv(df, path=csv_path):
    if os.path.isfile(path):
        df_old = pd.read_csv(path, index_col=0)
        df = df_old.append(df)
    df.to_csv(path)


def send_email(message, subject, attachment_path=None, to="dapr@nmbu.no"):
    with yagmail.SMTP(user=email_user, password=email_pw) as connection:
        connection.send(
            to=to, subject=subject, contents=message, attachments=attachment_path
        )


def send_dataset():
    # Should run every day at 23.59
    try:
        prefix = "./" + str(datetime.utcnow().date())
        new_path = prefix + csv_path.split("./")[-1]

        # copy file
        os.rename(csv_path, new_path)

        # Send dataset
        message = f"dataset for {str(datetime.utcnow().date())}"
        subject = f"dataset for {str(datetime.utcnow().date())}"
        send_email(message=None, subject=subject, attachment_path=new_path)
    except Exception as e:
        send_email(
            message=str(e),
            subject=f"Error sending data {str(datetime.utcnow().date())}",
        )


def collect_and_store():
    """Should run every hour
    """
    failures = 0
    error = ""
    while failures < 5:
        try:
            df = get_last_hour(bbox=oslo_bbox)
            append_to_csv(df=df)
            return
        except Exception as e:
            error = str(e)
            failures += 1
    send_email(
        subject=f"Error collecting data {str(datetime.utcnow().date())}", message=error
    )


if __name__ == "__main__":
    schedule.every(1).hour.at(":00").do(collect_and_store)
    schedule.every(1).day.at("23:55").do(send_dataset)

    while True:
        schedule.run_pending()
        time.sleep(1)
