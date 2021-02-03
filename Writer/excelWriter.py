import pandas as pd
import datetime as dt


class ExcelWriter:
    def __init__(self):
        pass

    @staticmethod
    def df_to_excel(path, *args):
        path = path + dt.datetime.today().strftime("%d_%m_%Y") + ".xlsx"
        writer = pd.ExcelWriter(path, engine='xlsxwriter')

        for df in args:
            df.to_excel(excel_writer=writer, sheet_name=df.name)

        writer.save()
