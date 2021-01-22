import json
import pandas as pd
import logging

from Datas.csvManager import CsvManager
from Connector.mongoDB import MongoConnector


class LogParser(CsvManager):
    def __init__(self, path, log_col_header):
        super().__init__(path=path)
        self.log_col_header = log_col_header
        self.matched_vh_input = list()
        self.matched_vh_output = list()
        self.unmatched_vh_input = list()

    def parse_log(self):
        df_logs = self.csv_to_dataframe()

        for log in df_logs[self.log_col_header]:
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
        for index, row in df_input.iterrows():
            mgo_data = mgo_connector.mongo_get_from_key(client=client, key_id=index)
            row["salerType"] = ""
            row["country"] = ""
            if mgo_data is not None:
                if "saler" in mgo_data.keys() and "type" in mgo_data["saler"].keys():
                    df_input.loc[index, "salerType"] = mgo_data["saler"]["type"]
                if "address" in mgo_data.keys() and "country" in mgo_data["address"].keys():
                    df_input.loc[index, "country"] = mgo_data["address"]["country"]

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
