import os
from typing import List

from nsaph_utils.utils.io_utils import DownloadTask

from gridmet_ds_def import GridmetContext, GridmetVariable
from gridmet_task import GridmetTask


class Gridmet:
    """
    Main class, describes the whole download and processing job for climate data
    Executing pipelines through this class requires a collection of shape files
    corresponding to geographies for which data is aggregated
    (for example, zip code areas or counties).

    The data has to be placed in teh following directory structure:
    ${year}/${geo_type: zip|county|etc.}/${shape:point|polygon}/

    Which geography is used is defined by `geography` argument that defaults
    to "zip". Only actually used geographies must have their shape files
    for the years actually used.
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
