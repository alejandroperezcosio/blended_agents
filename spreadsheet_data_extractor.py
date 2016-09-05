import csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from config import Config

config = Config(file('conf.cfg'))

# Connects to google drive and find the spreadsheet
spreadsheet = config.spreadsheet
credentials = ServiceAccountCredentials.from_json_keyfile_name(spreadsheet.credentials, spreadsheet.scopes)
gc = gspread.authorize(credentials)
worksheet_file = gc.open(spreadsheet.file)

# Gets values from spreadsheet and writes it in a local file
def get_csv(spreadsheet_sheet, output_csv):
    sheet = worksheet_file.worksheet(spreadsheet_sheet)
    sheet_vals = sheet.get_all_values()

    with open(output_csv, 'w') as local_csv:
        writer = csv.writer(local_csv)
        writer.writerows(sheet_vals)

get_csv(spreadsheet.agents_sheet, config.local_csv.agents)
get_csv(spreadsheet.tenants_sheet, config.local_csv.tenants)
get_csv(spreadsheet.queues_sheet, config.local_csv.queues)
