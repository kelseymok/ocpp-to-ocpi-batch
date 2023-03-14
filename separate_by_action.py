from typing import Dict

from helpers import recreate_dir
import pandas as pd
import json


source_directory = "data/input"
target_directory = "data/input_sep_by_action"

recreate_dir(target_directory)



actions = [
    "Heartbeat",
    "BootNotification",
    "StartTransaction",
    "MeterValues",
    "StopTransaction"
]

message_types = {
    "Request": 2,
    "Response": 3
}


def convert_body_to_dict(x: Dict):
    x["body"] = json.loads(x["body"])
    return x

df = pd.read_json(f"{source_directory}/1678731740.json")
for a in actions:
    print(f"Processing action: {a}")
    for m_name, m in message_types.items():
        tmp_df = df[(df["message_type"] == m) & (df["action"] == a)]
        print(tmp_df.head())
        tmp_df.to_json(f"{target_directory}/{a}{m_name}.json", orient="records") # Outputs straight up json
