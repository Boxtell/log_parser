import os

from Parser.logParser import LogParser
from Writer.excelWriter import ExcelWriter
from Connector.environement import EnvConnector

envConnector = EnvConnector(env_path="/Users/admin/Desktop/Reezocar/analyse_data_cgi/data/Input")
list_of_inputs = envConnector.get_directory()
logParser = LogParser()

for file_name in list_of_inputs:
    name, extension = os.path.splitext(file_name)
    logParser.parse_log(log_file_path="/Users/admin/Desktop/Reezocar/analyse_data_cgi/data/Input/{}".format(file_name),
                        log_col_header="message",
                        file_type=extension)

df_matched_input, df_matched_output, df_unmatched_input = logParser.concat_results("internal_id")

df_matched_input = logParser.enrich_data(df_matched_input)
df_matched_output = logParser.enrich_data(df_matched_output)
df_unmatched_input = logParser.enrich_data(df_unmatched_input)

ExcelWriter.df_to_excel(
    "/Users/admin/Desktop/Reezocar/analyse_data_cgi/data/Output/output_",
    df_matched_input,
    df_matched_output,
    df_unmatched_input
)
