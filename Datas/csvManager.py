import pandas as pd


class InputManager(object):
    def source_to_dataframe(self, path, source_type):
        method_name = source_type + "_reader"
        method = getattr(self, method_name[1:], lambda: "Invalid source")

        return method(path)

    def excel_reader(self, path):
        return pd.read_excel(path, engine="openpyxl")

    def csv_reader(self, path):
        return pd.read_csv(filepath_or_buffer=path, error_bad_lines=False)
