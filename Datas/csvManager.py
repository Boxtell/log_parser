import pandas as pd


class CsvManager:
    def __init__(self, path):
        self.csv_path = path

    def csv_to_dataframe(self):
        return pd.read_csv(filepath_or_buffer=self.csv_path)

