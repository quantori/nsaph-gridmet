from netCDF4._netCDF4 import Dataset

from gridmet_ds_def import RasterizationStrategy, GridmetVariable


class ComputeGridmetTask:
    def __init__(self, year: int,
                 variable: GridmetVariable,
                 infile: str,
                 outfile:str,
                 strategy: RasterizationStrategy,
                 shapefile:str):
        self.year = year
        self.infile = infile
        self.outfile = outfile
        self.variable = variable
        self.strategy = strategy
        self.shapefile = shapefile


    @classmethod
    def get_variable(cls, dataset: Dataset,  variable: GridmetVariable):
        standard_name = variable.value
        for var in dataset.variables.values():
            if var["standard_name"] == standard_name:
                return var["name"]





