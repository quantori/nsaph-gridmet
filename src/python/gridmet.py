import os
from typing import List

from nsaph_utils.utils.io_utils import DownloadTask

from gridmet_ds_def import GridmetContext, GridmetVariable


class Gridmet:
    """
    Main class, describes the whole download and processing job for climate data
    """
    base_metdata_url = "https://www.northwestknowledge.net/metdata/data/"
    url_pattern = base_metdata_url + "{}_{:d}.nc"

    def __init__(self, context: GridmetContext = None):
        """
        Creates a new instance
        :param context: An optional GridmetContext object, if not specified,
            then it is constructed from the command line arguments
        """
        if not context:
            context = GridmetContext(__doc__)
        self.context = context
        self.download_tasks = self.collect_download_tasks(self.context)

    @classmethod
    def get_url(cls, year:int, variable: GridmetVariable) -> str:
        return cls.url_pattern.format(variable.value, year)

    @classmethod
    def collect_download_tasks(cls, context: GridmetContext) -> List:
        destination = context.raw_downloads
        if not os.path.isdir(destination):
            os.makedirs(destination)
        tasks = [
            DownloadTask(os.path.join(url.split('/')[-1]), [url])
            for url in [
                cls.get_url(y, v)
                for y in context.years for v in context.variables
            ]
        ]
        return tasks


if __name__ == '__main__':
    gridmet = Gridmet()
