import logging
import datetime

import pandas as pd

from data_source.reonline_data import ReOnlineData

logger = logging.getLogger(__name__)


class ReOnlineFlow:
    def __init__(self):
        self._reonline_data_source = ReOnlineData()

    def add_sqm(self, excel_path: str):
        assert "xlsx" in excel_path[-5:] or "xlsb" in excel_path[-5:]
        if "xlsb" in excel_path[-5:]:
            df = pd.read_excel(excel_path, engine='pyxlsb')
        else:  # "xlsx" in excel_path[-5:]
            df = pd.read_excel(excel_path)

        assert 'Link' in df.columns
        for row, index in df.iterrows():
            try:
                df.loc[index, 'sqm'] = self._reonline_data_source.get_sqm(df['Link'])

            except Exception as e:
                logger.error(f"something failed. SAVING!: {e}")

            finally:
                df.to_excel(
                    f'{excel_path}_sqm_enrich_{datetime.datetime.now().strftime("%d%m%Y-%H%M")}.xlsx',
                    index=False)
                logger.info("saved successfully")

if __name__ == '__main__':
    r = ReOnlineFlow()
    r.add_sqm(excel_path=r"../byhand/dvg_reo.xlsx_spitogatos_comparisson_09102025-1401.xlsx")
