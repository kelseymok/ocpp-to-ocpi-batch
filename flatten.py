import os
from typing import Dict

from helpers import recreate_dir
import pandas as pd
import json

source_directory = "data/input_sep_by_action"
target_directory = "data/flattened"

recreate_dir(target_directory)


actions = [
    "Heartbeat",
    "BootNotification",
    "StartTransaction",
    "MeterValues",
    "StopTransaction"
]

def convert_body_to_dict(x: Dict):
    x["body"] = json.loads(x["body"])
    return x


def heartbeat_request_normalizer(data):
    df = pd.json_normalize(data)
    df.columns = [c.replace(".", "_").lower() for c in df.columns]
    return df

def heartbeat_response_normalizer(data):
    df = pd.json_normalize(data)
    df.columns = [c.replace(".", "_").lower() for c in df.columns]
    return df


def boot_notification_request_normalizer(data):
    df = pd.json_normalize(data)
    df.columns = [c.replace(".", "_").lower() for c in df.columns]
    return df


def boot_notification_response_normalizer(data):
    df = pd.json_normalize(data)
    df.columns = [c.replace(".", "_").lower() for c in df.columns]
    return df


def start_transaction_request_normalizer(data):
    return pd.json_normalize(
        data,
    ).rename(columns={
        "body.connector_id": "connector_id",
        "body.id_tag": "id_tag",
        "body.meter_start": "meter_start",
        "body.timestamp": "timestamp",
    })


def start_transaction_response_normalizer(data):
    df = pd.json_normalize(data)
    df.columns = [c.replace(".", "_").lower() for c in df.columns]
    return df


def meter_values_request_normalizer(data):
    df = pd.json_normalize(
        data,
        record_path=['body', 'meter_value', 'sampled_value'],
        meta=[
            'message_id',
            'charge_point_id',
            'action',
            'write_timestamp',
            'write_timestamp_epoch',
            ['body', 'connector_id'], ['body', 'transaction_id'],
            ['body', 'meter_value', 'timestamp']
        ]
    ).rename(columns={
        "body.connector_id": "connector_id",
        "body.meter_value.timestamp": "timestamp",
        "body.transaction_id": "transaction_id"}
    )
    df["value"] = df["value"].astype(float)
    return df


def meter_values_response_normalizer(data):
    df = pd.json_normalize(data)
    df.columns = [c.replace(".", "_").lower() for c in df.columns]
    return df


def stop_transaction_request_normalizer(data):
    df = pd.json_normalize(
        data,
    ).rename(columns={
        "body.connector_id": "connector_id",
        "body.id_tag": "id_tag",
        "body.meter_stop": "meter_stop",
        "body.transaction_id": "transaction_id",
        "body.reason": "reason",
        "body.timestamp": "timestamp",
    })

    return df


def stop_transaction_response_normalizer(data):
    df = pd.json_normalize(data)
    df.columns = [c.replace(".", "_").lower() for c in df.columns]
    return df


def normalizers():
    return {
        "HeartbeatRequest": heartbeat_request_normalizer,
        "BootNotificationRequest": boot_notification_request_normalizer,
        "StartTransactionRequest": start_transaction_request_normalizer,
        "MeterValuesRequest": meter_values_request_normalizer,
        "StopTransactionRequest": stop_transaction_request_normalizer,
        "HeartbeatResponse": heartbeat_response_normalizer,
        "BootNotificationResponse": boot_notification_response_normalizer,
        "StartTransactionResponse": start_transaction_response_normalizer,
        "MeterValuesResponse": meter_values_response_normalizer,
        "StopTransactionResponse": stop_transaction_response_normalizer
    }


for file in os.listdir(source_directory):
    with open(f"{source_directory}/{file}") as f:
        input = json.load(f)
        data = [convert_body_to_dict(x) for x in input]
        t = file.replace(".json", "")
        normalizer = normalizers()[t]
        ind_df = (normalizer(d) for d in data)
        a_df = pd.concat(ind_df, ignore_index=True)
        a_df.to_json(path_or_buf=f"{target_directory}/{t}.json", orient="records")
