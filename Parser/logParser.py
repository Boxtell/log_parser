import json
import pandas as pd

from Datas.csvManager import CsvManager


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
                self.matched_vh_input.append(df_input)
                df_output = pd.json_normalize(data['output'])
                self.matched_vh_output.append(df_output)
            else:
                self.unmatched_vh_input.append(df_input)

    def concat_results(self):
        df_matched_vh_input = pd.concat(self.matched_vh_input)
        df_matched_vh_output = pd.concat(self.matched_vh_output)
        df_unmatched_vh_input = pd.concat(self.unmatched_vh_input)

        df_matched_vh_input.name = "matched_vh_input"
        df_matched_vh_output.name = "matched_vh_output"
        df_unmatched_vh_input.name = "unmatched_vh_input"

        return df_matched_vh_input, df_matched_vh_output, df_unmatched_vh_input
