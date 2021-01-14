import os
from fusetools.gsuite_tools import GSheets
from airflow.hooks.base_hook import BaseHook


class GSheetsHook(BaseHook):
    def __init__(self):
        self.auth = \
            GSheets.create_service_serv_acct(
                member_acct_email=os.environ['fc_email'],
                token_path='/home/collier/Downloads/tokennew.json'
            )

    def delete_data(self, sheet_id, tab_id, idx_start=0, idx_end=999):
        GSheets.delete_google_sheet_data(
            spreadsheet_id=sheet_id,
            sheet_id=tab_id,
            idx_start=idx_start,
            idx_end=idx_end,
            credentials=self.auth[1],
            dimension="ROWS"
        )

    def push_data(self, sheet_id, tab_name, df):
        GSheets.update_google_sheet_df(
            spreadsheet_id=sheet_id,
            df=df,
            data_range=f'''{tab_name}!A1''',
            credentials=self.auth[1],
            header=True
        )
