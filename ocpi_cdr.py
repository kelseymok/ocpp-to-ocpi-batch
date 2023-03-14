from pyspark.sql import SparkSession
import pandas as pd
import json
from dateutil import parser
from dateutil.tz import UTC

from pyspark.sql.functions import col

spark = SparkSession.builder.appName('app').getOrCreate()

source_directory = "data/flattened"


stop_transaction_request_df_pandas = pd.read_json(f"{source_directory}/StopTransactionRequest.json")
stop_transaction_request_df = spark.createDataFrame(stop_transaction_request_df_pandas)

start_transaction_request_df_pandas = pd.read_json(f"{source_directory}/StartTransactionRequest.json")
start_transaction_request_df = spark.createDataFrame(start_transaction_request_df_pandas)

start_transaction_response_df_pandas = pd.read_json(f"{source_directory}/StartTransactionResponse.json")
start_transaction_response_df = spark.createDataFrame(start_transaction_response_df_pandas)


stop_transaction_event = {
    "message_id": "92f95e85-21d2-4779-8bc9-dde09346a768",
    "message_type": 2,
    "charge_point_id": "de9f4da4-3ec7-4cbb-a290-68ca4abab2b7",
    "action": "StopTransaction",
    "write_timestamp": "2023-01-01T18:16:26.311155+00:00",
    "body": "{\"meter_stop\": 51333.899999999994, \"timestamp\": \"2023-01-01T18:16:26.311155+00:00\", \"transaction_id\": 5, \"reason\": null, \"id_tag\": \"1c69b06e-376d-446f-8d51-e33f3d053a60\", \"transaction_data\": null}",
    "write_timestamp_epoch": 1672596986311
}
stop_transaction_event["body"] = json.loads(stop_transaction_event["body"])

# Find the related StartTransaction Response for the id
start_transaction_message_id = start_transaction_response_df \
    .filter(start_transaction_response_df.body_transaction_id == stop_transaction_event["body"]["transaction_id"]) \
    .select(col("message_id")) \
    .first()["message_id"]


#Find the start transaction request for the message_id
start_transaction_request = start_transaction_request_df \
    .filter(start_transaction_request_df.message_id == start_transaction_message_id) \
    .first()


# Build OCPI object
ocpi = {
    "start_date_time": start_transaction_request["timestamp"].astimezone(UTC).isoformat(), # from request
    "end_date_time": stop_transaction_event["body"]["timestamp"], # from stop transaction request
    "total_energy": stop_transaction_event["body"]["meter_stop"],
    "total_time": (parser.parse(stop_transaction_event["body"]["timestamp"]) - start_transaction_request["timestamp"].astimezone(UTC)).total_seconds() / 3600
}

print("OCPI Object")
print(ocpi)