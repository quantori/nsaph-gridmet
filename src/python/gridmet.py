import os
from typing import List

from nsaph_utils.utils.io_utils import DownloadTask

from gridmet_ds_def import GridmetContext, GridmetVariable
from gridmet_task import GridmetTask


class Gridmet:
    """
    Main class, describes the whole download and processing job for climate data
    """

    def __init__(self, context: GridmetContext = None):
        """
        Creates a new instance
        :param context: An optional GridmetContext object, if not specified,
            then it is constructed from the command line arguments
        """
        if not context:
            context = GridmetContext(__doc__)
        self.context = context
        self.tasks = self.collect_tasks(self.context)

    @classmethod
    def collect_tasks(cls, context: GridmetContext) -> List:
        tasks = [
            GridmetTask(context, y, v)
                for y in context.years for v in context.variables
        ]
        return tasks


if __name__ == '__main__':
    gridmet = Gridmet()
