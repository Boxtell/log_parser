from Parser.logParser import LogParser
from Writer.excelWriter import ExcelWriter

# TODO: Passer en cmd args ou en var d'env le path et le column name
logParser = LogParser(path="/Users/admin/Desktop/extractMkmd.csv",
                      log_col_header="message")
logParser.parse_log()
ExcelWriter.df_to_excel(logParser.concat_results())

