import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd
import requests
from matplotlib.dates import DateFormatter, MonthLocator


class PlotClimbs:
    """ Plots contents of climb TopLogger sub-API"""

    URL_CLIMBS = 'https://api.toplogger.nu/v1/gyms/21/climbs/'

    _DF_DATES_GRADES_PICKLE_PATH = Path('df_dates_grades.pkl')
    _DF_CLIMBS_PICKLE_PATH = Path('df_climbs.pkl')

    def __init__(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info('Starting')
        self.climbs = self._load_climbs()


    def main(self):
        """Just do it"""
        self._climbs_grades()

    def _climbs_grades(self):
        """ Uses pandas and matplotlib to join daily data into monthly data and plot it as a bar plot."""
        fig, ax = self._get_fig_ax()

        df_dates_grades = self._load_dates_grades()
        df_dates_grades['grade_int'] = df_dates_grades.grade.astype(float)
        df_dates_grades['grade_int'] = df_dates_grades.grade_int.astype(int)

        df_dates_grades_int = df_dates_grades.groupby(['date', 'grade_int']).sum().reset_index()
        df_dates_grades_int.pivot(index='date', columns='grade_int').plot.area(ax=ax, stacked=True, legend=False, rot=0)

        ax.set_title('Routes per difficulty')
        lgd = fig.legend(loc='upper center', ncol=9)

        for txt in lgd.get_texts():
            txt.set_text(txt.get_text()[-2:-1])

        self._customise(ax)

        plt.show()
        fig.savefig('test.png')

    def _routes_avail_per_day(self, date_grade):
        date, grade = date_grade

        df_tmp = self.climbs[self.climbs.grade == grade]
        df_tmp = df_tmp[df_tmp.date_live_start <= date]
        df_tmp = df_tmp[df_tmp.date_live_end >= date]
        return df_tmp.id.count()

    @staticmethod
    def _customise(ax):
        ax.xaxis.set_major_locator(MonthLocator([1, 7]))
        ax.xaxis.set_major_formatter(DateFormatter('%Y-%m'))
        ax.set_ylabel('Climbs')
        ax.set_xlabel('Date')
        ax.xaxis.set_tick_params(rotation=30)  # This is to avoid d30 in month N overlapping d01 in month N+1
        ax.set_xlim(datetime(2018, 1, 1), datetime.today())
    @staticmethod
    def _get_fig_ax():
        fig, ax = plt.subplots()
        return fig, ax

    def _load_climbs(self) -> pd.DataFrame:
        if (df_pickle := self._load_pickle(self._DF_CLIMBS_PICKLE_PATH, age_max=timedelta(days=10))) is not None:
            return df_pickle

        logging.info('loading data from API')
        df = pd.DataFrame.from_dict(requests.get(self.URL_CLIMBS).json())

        df.date_live_start = df.date_live_start.astype('datetime64[s]')
        df.date_live_end = df.date_live_end.astype('datetime64[s]')
        df.loc[df.date_live_end.isna(), 'date_live_end'] = datetime.today()
        df.dropna(subset=['date_live_start'], inplace=True)

        logging.info('saving pickle')
        self._save_df_to_pickle(df, self._DF_CLIMBS_PICKLE_PATH)
        return df

    def _load_dates_grades(self) -> pd.DataFrame:
        if (df_pickle := self._load_pickle(self._DF_DATES_GRADES_PICKLE_PATH, age_max=timedelta(days=10))) is not None:
            return df_pickle

        date_min = self.climbs.date_live_start.dropna().min()
        date_max = self.climbs.date_live_start.dropna().max()

        df_dates = pd.date_range(date_min, date_max, freq='D', name='date').to_frame()

        grades = pd.Series(sorted(self.climbs.grade.unique()), name='grade')

        df = df_dates.merge(grades, how='cross')
        logging.info('index created')
        result = df.apply(self._routes_avail_per_day, raw=True, axis=1)
        logging.info('data joined')
        df['count'] = result
        logging.info('column added')

        self._save_df_to_pickle(df, self._DF_DATES_GRADES_PICKLE_PATH)

        return df

    @staticmethod
    def _load_pickle(f, age_max: Optional[timedelta] = None) -> Optional[pd.DataFrame]:
        """ Loads a pickle file if it exists, otherwise returns None.
        If age_max is not None, the pickle is only loaded if it is less than age_max old.
        """
        if f.exists():
            age = datetime.now() - datetime.fromtimestamp(f.stat().st_mtime)
            if age_max is None or age < age_max:
                logging.info(f'loading pickle {f} (age = {age}, limit={age_max})')
                df = pd.read_pickle(f)
                logging.info(f'loading pickle done')
                return df

    @staticmethod
    def _save_df_to_pickle(df, f):
        logging.info(f'saving pickle to {f}')
        df.to_pickle(str(f))


if __name__ == '__main__':
    PlotClimbs().main()
