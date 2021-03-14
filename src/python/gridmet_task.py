import csv
import os
from datetime import date, timedelta, datetime

import rasterio
from netCDF4._netCDF4 import Dataset
from nsaph_utils.utils.io_utils import DownloadTask, fopen
from rasterstats import zonal_stats

from gridmet_ds_def import RasterizationStrategy, GridmetVariable, \
    GridmetContext, Shape, Geography


class ComputeGridmetTask:
    def __init__(self, year: int,
                 variable: GridmetVariable,
                 infile: str,
                 outfile:str,
                 strategy: RasterizationStrategy,
                 shapefile:str,
                 geography: Geography):
        self.year = year
        self.infile = infile
        self.outfile = outfile
        self.variable = variable
        self.strategy = strategy
        self.shapefile = shapefile
        self.geography = geography


    @classmethod
    def get_variable(cls, dataset: Dataset,  variable: GridmetVariable):
        standard_name = variable.value
        for var in dataset.variables.values():
            if var["standard_name"] == standard_name:
                return var["name"]

    @staticmethod
    def combine(p, r1, r2):
        prop = r1['properties'][p]
        assert prop == r2['properties'][p]
        m1 = r1['properties']['mean']
        m2 = r2['properties']['mean']
        if m1 and m2:
            mean = (m1 + m2) / 2
        elif m2:
            mean = m2
        elif m1:
            raise AssertionError("m1 && !m2")
        else:
            mean = None
        return mean, prop


    def execute(self, mode:str = "w"):
        ds = Dataset(self.infile)
        with rasterio.open(self.infile) as rio:
            affine = rio.transform
        origin = date(1900, 1, 1)
        days = ds["day"][:]
        t0 = datetime.now()
        var = self.get_variable(ds, self.variable)
        p = self.geography.value.upper()
        with fopen(self.outfile, mode) as out:
            writer = csv.writer(out, delimiter=',', quoting=csv.QUOTE_NONE)
            writer.writerow([self.variable, "date", self.geography.value])
            for idx in range(0, len(days)):
                day = days[idx]
                dt = origin + timedelta(days=day)
                layer = ds[var][idx, :, :]
                print(dt, end='')
                t1 = datetime.now()
                l = None
                if self.strategy in [RasterizationStrategy.DEFAULT,
                                     RasterizationStrategy.COMBINED]:
                    stats1 = zonal_stats(self.shapefile, layer, stats="mean",
                                         affine=affine, geojson_out=True,
                                         all_touched=False)
                    l = len(stats1)
                if self.strategy in [RasterizationStrategy.ALL_TOUCHED,
                                     RasterizationStrategy.COMBINED]:
                    stats2 = zonal_stats(self.shapefile, layer, stats="mean",
                                         affine=affine, geojson_out=True,
                                         all_touched=False)
                    l = len(stats2)

                for i in range(0, l):
                    if self.strategy == RasterizationStrategy.COMBINED:
                        mean, prop = self.combine(p, stats1[i], stats2[i])
                    elif self.strategy == RasterizationStrategy.DEFAULT:
                        mean = stats1[i]['properties']['mean']
                        prop = stats1[i]['properties'][p]
                    else:
                        mean = stats2[i]['properties']['mean']
                        prop = stats2[i]['properties'][p]
                writer.writerow([mean, dt.strftime("%Y-%m-%d"), prop])
            t3 = datetime.now()
            t = datetime.now() - t0
            print(" \t{} [{}]".format(str(t3 - t1), str(t)))



class GridmetTask:
    """
    Defines a task to download and process data for a single year and variable
    Instances of this class can be used to parallelize processing
    """
    base_metdata_url = "https://www.northwestknowledge.net/metdata/data/"
    url_pattern = base_metdata_url + "{}_{:d}.nc"

    @classmethod
    def get_url(cls, year:int, variable: GridmetVariable) -> str:
        return cls.url_pattern.format(variable.value, year)

    @classmethod
    def destination_file_name(cls, context: GridmetContext,
                              year: int,
                              variable: GridmetVariable):
        g = context.geography.value
        f = "{}_{}_{:d}.csv".format(variable.value, g, year)
        if context.compress:
            f += ".gz"

    @classmethod
    def find_shape_file(cls, context: GridmetContext, year: int, shape: Shape):
        shape_file = None
        parent_dir = context.shapes
        y = year
        while y > 1999:
            d = os.path.join(parent_dir, "{:d}".format(y))
            if os.path.isdir(d):
                f = "{}/{}/ESRI{:02d}USZIP5_POLY_WGS84.shp".format(
                    context.geography.value, shape.value,  y - 2000)
                shape_file = os.path.join(d, f)
                break
            y -= 1
        if shape_file is None:
            raise Exception(
                "Could not find ZIP shape file for year {:d} or earlier"
                    .format(year))
        return shape_file

    def __init__(self, context: GridmetContext,
                 year: int,
                 variable: GridmetVariable):
        destination = context.raw_downloads
        if not os.path.isdir(destination):
            os.makedirs(destination)

        url = self.get_url(year, variable)
        download_target = os.path.join(destination, url.split('/')[-1])
        self.download_task = DownloadTask(download_target, [url])

        destination = context.destination
        if not os.path.isdir(destination):
            os.makedirs(destination)

        result = self.destination_file_name(context, year, variable)

        self.compute_tasks = [
            ComputeGridmetTask(year,
                               variable,
                               download_target,
                               result,
                               context.strategy,
                               shape_file,
                               context.geography)
            for shape_file in [
                self.find_shape_file(context, year, shape)
                for shape in context.shapes
            ]
        ]

