from Parser.logParser import LogParser
from Writer.excelWriter import ExcelWriter

# TODO: Passer en cmd args ou en var d'env le path et le column name
logParser = LogParser(path="/Users/admin/Desktop/extractMkmd.csv",
                      log_col_header="message")
logParser.parse_log()

df_matched_input, df_matched_output, df_unmatched_input = logParser.concat_results("internal_id")

df_matched_input = logParser.enrich_data(df_matched_input)
df_matched_output = logParser.enrich_data(df_matched_output)
df_unmatched_input = logParser.enrich_data(df_unmatched_input)

ExcelWriter.df_to_excel(df_matched_input, df_matched_output, df_unmatched_input)
