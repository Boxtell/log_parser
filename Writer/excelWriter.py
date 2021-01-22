import pandas as pd


class ExcelWriter:
    def __init__(self):
        pass

    @staticmethod
    def df_to_excel(*args):
        writer = pd.ExcelWriter('/Users/admin/Desktop/mkmd.xlsx', engine='xlsxwriter')

        for df in args:
            df.to_excel(excel_writer=writer, sheet_name=df.name)

        writer.save()
