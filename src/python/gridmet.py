from gridmet_ds_def import GridmetContext


class Gridmet:
    """
    Main class, describes the whole download job
    """
    def __init__(self, context: GridmetContext = None):
        """
        Creates a new instance
        :param context: An optional AQSContext object, if not specified,
            then it is constructed from the command line arguments
        """
        if not context:
            context = GridmetContext(__doc__)
        self.context = context
        self.download_tasks = None
