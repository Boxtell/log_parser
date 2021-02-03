import json
import pandas as pd
import logging
import time

from Datas.csvManager import InputManager
from Connector.mongoDB import MongoConnector


class LogParser(InputManager):
    def __init__(self):
        super().__init__()
        self.matched_vh_input = list()
        self.matched_vh_output = list()
        self.unmatched_vh_input = list()

    def parse_log(self, log_file_path, log_col_header, file_type):
        df_logs = self.source_to_dataframe(path=log_file_path, source_type=file_type)

        if log_col_header in df_logs:
            for log in df_logs[log_col_header]:
                data = json.loads(log)
                df_input = pd.DataFrame(data["input"], index=[0])

                if "output" in data.keys():
                    df_output = pd.json_normalize(data['output'])
                    try:
                        df_output["internal_id"] = df_input["internal_id"]
                        self.matched_vh_input.append(df_input)
                        self.matched_vh_output.append(df_output)
                    except KeyError:
                        logging.warning("skipped row, internal id missing in mkmd inputs")
                        continue
                else:
                    self.unmatched_vh_input.append(df_input)

    def enrich_data(self, df_input):
        mgo_connector = MongoConnector()
        client = mgo_connector.mongo_connect("spiders")
        df_input["salerType"] = ""
        df_input["country"] = ""
        df_input["mileage"] = ""
        df_input["price"] = ""
        for index, row in df_input.iterrows():
            mgo_data = mgo_connector.mongo_get_from_key(client=client, key_id=index)
            row["salerType"] = ""
            row["country"] = ""
            row["mileage"] = ""
            row["price"] = ""
            if mgo_data is not None:
                if "saler" in mgo_data.keys() and "type" in mgo_data["saler"].keys():
                    df_input.loc[index, "salerType"] = mgo_data["saler"]["type"]
                if "address" in mgo_data.keys() and "country" in mgo_data["address"].keys():
                    df_input.loc[index, "country"] = mgo_data["address"]["country"]
                if "mileage" in mgo_data.keys():
                    df_input.loc[index, "mileage"] = mgo_data["mileage"]
                if "price" in mgo_data.keys() and "price" in mgo_data["price"].keys():
                    df_input.loc[index, "price"] = mgo_data["price"]["price"]

        return df_input

    def concat_results(self, index_col):
        df_matched_vh_input = pd.concat(self.matched_vh_input)
        df_matched_vh_output = pd.concat(self.matched_vh_output)
        df_unmatched_vh_input = pd.concat(self.unmatched_vh_input)

        df_matched_vh_input.name = "matched_vh_input"
        df_matched_vh_output.name = "matched_vh_output"
        df_unmatched_vh_input.name = "unmatched_vh_input"

        df_matched_vh_input.set_index(index_col, inplace=True)
        df_matched_vh_output.set_index(index_col, inplace=True)
        df_unmatched_vh_input.set_index(index_col, inplace=True)

        return df_matched_vh_input, df_matched_vh_output, df_unmatched_vh_input
