import csv
import os
from datetime import date, timedelta, datetime
from typing import List
from abc import ABC, abstractmethod

from netCDF4._netCDF4 import Dataset
from nsaph_utils.utils.io_utils import DownloadTask, fopen, as_stream
from rasterstats import zonal_stats, point_query
from shapely.geometry import Point

from gridmet_ds_def import RasterizationStrategy, GridmetVariable, \
    GridmetContext, Shape, Geography
from gridmet_tools import find_shape_file, get_nkn_url, get_variable, get_days, \
    get_affine_transform


class ComputeGridmetTask(ABC):
    """
    An abstract class for a computational task that processes data in
    Unidata netCDF (Version 4) format:
    https://www.unidata.ucar.edu/software/netcdf/
    """

    origin = date(1900, 1, 1)

    def __init__(self, year: int,
                 variable: GridmetVariable,
                 infile: str,
                 outfile:str):
        """

        :param year: year
        :param variable: Gridemt band (variable)
        :param infile: File with source data in  NCDF4 format
        :param outfile: Resulting CSV file
        """

        self.year = year
        self.infile = infile
        self.outfile = outfile
        self.variable = variable
        self.affine = get_affine_transform(self.infile)

    @classmethod
    def get_variable(cls, dataset: Dataset,  variable: GridmetVariable):
        return get_variable(dataset, variable.value)

    @abstractmethod
    def get_key(self):
        pass

    def prepare(self):
        print("{} => {}".format(self.infile, self.outfile))
        ds = Dataset(self.infile)
        days = get_days(ds)
        var = self.get_variable(ds, self.variable)
        return ds, days, var

    def execute(self, mode:str = "w"):
        """
        Executes computational task

        :param mode: mode to use opening result file
        :type mode: str
        :return:
        """

        ds, days, var = self.prepare()
        key = self.get_key()
        t0 = datetime.now()
        with fopen(self.outfile, mode) as out:
            writer = csv.writer(out, delimiter=',', quoting=csv.QUOTE_NONE)
            writer.writerow([self.variable.value, "date", key.lower()])
            for idx in range(0, len(days)):
                day = days[idx]
                layer = ds[var][idx, :, :]
                t1 = datetime.now()
                self.compute_one_day(writer, day, layer, key)
                out.flush()
                t3 = datetime.now()
                t = datetime.now() - t0
                print(" \t{} [{}]".format(str(t3 - t1), str(t)))
        return

    @abstractmethod
    def compute_one_day(self, writer, day, layer, key):
        """
        Computes required statistics for a single day.
        This method is called by `execute()` and is implemented in
        specific subclasses

        :param writer: CSV Writer to output the result
        :param day: day
        :param layer: layer, corresponding to the day
        :param key: unique identifier for a geography
        :return: Nothing
        """

        pass


class ComputeShapesTask(ComputeGridmetTask):
    """
    Class describes a compute task to aggregate data over geography shapes

    The data is expected in
    .. _Unidata netCDF (Version 4) format: https://www.unidata.ucar.edu/software/netcdf/
    """

    def __init__(self, year: int, variable: GridmetVariable, infile: str,
                 outfile: str, strategy: RasterizationStrategy, shapefile: str,
                 geography: Geography):
        """

        :param year: year
        :param variable: Gridemt band (variable)
        :param infile: File with source data in  NCDF4 format
        :param outfile: Resulting CSV file
        :param strategy: Rasterization strategy to use
        :param shapefile: Shapefile for used collection of geographies
        :param geography: Type of geography, e.g. zip code or county
        """

        super().__init__(year, variable, infile, outfile)
        self.strategy = strategy
        self.shapefile = shapefile
        self.geography = geography

    def get_key(self):
        return self.geography.value.upper()

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


    def compute_one_day(self, writer, day, layer, key):
        dt = self.origin + timedelta(days=day)
        print(dt, end='')
        l = None
        if self.strategy in [RasterizationStrategy.default,
                             RasterizationStrategy.combined]:
            stats1 = zonal_stats(self.shapefile, layer, stats="mean",
                                 affine=self.affine, geojson_out=True,
                                 all_touched=False)
            l = len(stats1)
        if self.strategy in [RasterizationStrategy.all_touched,
                             RasterizationStrategy.combined]:
            stats2 = zonal_stats(self.shapefile, layer, stats="mean",
                                 affine=self.affine, geojson_out=True,
                                 all_touched=False)
            l = len(stats2)

        for i in range(0, l):
            if self.strategy == RasterizationStrategy.combined:
                mean, prop = self.combine(key, stats1[i], stats2[i])
            elif self.strategy == RasterizationStrategy.default:
                mean = stats1[i]['properties']['mean']
                prop = stats1[i]['properties'][key]
            else:
                mean = stats2[i]['properties']['mean']
                prop = stats2[i]['properties'][key]
            writer.writerow([mean, dt.strftime("%Y-%m-%d"), prop])
        return


class ComputePointsTask(ComputeGridmetTask):
    """
    Class describes a compute task to assign data to a collection of points

    The data is expected in
    .. _Unidata netCDF (Version 4) format: https://www.unidata.ucar.edu/software/netcdf/
    """

    def __init__(self, year: int,
                 variable: GridmetVariable,
                 infile: str,
                 outfile:str,
                 points_file:str,
                 coordinates: List,
                 metadata: List):
        """

        :param year: year
        :param variable: Gridemt band (variable)
        :param infile: File with source data in  NCDF4 format
        :param outfile: Resulting CSV file
        :param points_file: path to a file containing coordinates of points
            in csv format.
        :param coordinates: A two element list of column names in csv
            corresponding to coordinates
        :param metadata: A list of column names in csv that should be
            interpreted as metadata (e.g. ZIP, site_id, etc.)
        """

        super().__init__(year, variable, infile, outfile)
        self.points = points_file
        assert len(coordinates) == 2
        self.coordinates = coordinates
        self.metadata = metadata

    def get_key(self):
        return self.metadata[0]

    def compute_one_day(self, writer, day, layer, key):
        dt = self.origin + timedelta(days=day)
        print(dt, end='')
        with fopen(self.points, "r") as points:
            reader = csv.DictReader(points)
            for row in reader:
                x = float (row[self.coordinates[0]])
                y = float (row[self.coordinates[1]])
                metadata = [row[p] for p in self.metadata]
                point = Point(x,y)
                stats = point_query(point, layer, affine=self.affine)
                mean = stats[0]
                writer.writerow([mean, dt.strftime("%Y-%m-%d")] + metadata)
        return





class DownloadGridmetTask:
    """
    Task to download source file in NCDF4 format
    """

    BLOCK_SIZE = 65536

    @classmethod
    def get_url(cls, year:int, variable: GridmetVariable) -> str:
        """
        Constructs URL given a year and band

        :param year: year
        :param variable: Gridmet band (variable)
        :return: URL for download
        """
        return get_nkn_url(variable.value, year)

    def __init__(self, year: int,
                 variable: GridmetVariable,
                 destination: str):
        """
        :param year: year
        :param variable: Gridmet band (variable)
        :param destination: Destination directory for all downloads
        """
        if not os.path.isdir(destination):
            os.makedirs(destination)

        url = self.get_url(year, variable)
        target = os.path.join(destination, url.split('/')[-1])
        self.download_task = DownloadTask(target, [url])

    def target(self):
        """
        :return: File path for downloaded data
        """
        return self.download_task.destination

    def execute(self):
        """
        Executes the task
        :return: None
        """

        print(str(self.download_task))
        if self.download_task.is_up_to_date():
            print("Up to date")
            return
        buffer = bytearray(self.BLOCK_SIZE)
        with fopen(self.target(), "wb") as writer, \
                as_stream(self.download_task.urls[0]) as reader:
            n = 0
            while True:
                ret = reader.readinto(buffer)
                if not ret:
                    break
                writer.write(buffer[:ret])
                n += 1
                if (n % 20) == 0:
                    print("*", end='')
        return


class GridmetTask:
    """
    Defines a task to download and process data for a single year and variable
    Instances of this class can be used to parallelize processing
    """

    @classmethod
    def destination_file_name(cls, context: GridmetContext,
                              year: int,
                              variable: GridmetVariable):
        """
        Constructs a file name for a given set of parameters

        :param context: Configuration object for the pipeline
        :param year: year
        :param variable: Gridmet band (variable)
        :return: `variable_geography_year.csv[.gz]`
        """
        g = context.geography.value
        f = "{}_{}_{:d}.csv".format(variable.value, g, year)
        if context.compress:
            f += ".gz"
        return os.path.join(context.destination, f)

    @classmethod
    def find_shape_file(cls, context: GridmetContext, year: int, shape: Shape):
        """
        Finds shapefile for a given type of geographies for the
        closest available year

        :param context: Configuration object for the pipeline
        :param year: year
        :param shape: Shape type
        :return: a shape file for a given year if it exists or for the latest
            year before the given
        """

        parent_dir = context.shapes_dir
        geo_type = context.geography.value
        shape_type = shape.value
        return find_shape_file(parent_dir, year, geo_type, shape_type)

    def __init__(self, context: GridmetContext,
                 year: int,
                 variable: GridmetVariable):
        """
        :param context: Configuration object for the pipeline
        :param year: year
        :param variable: Gridmet band (variable)
        """
        destination = context.raw_downloads
        self.download_task = DownloadGridmetTask(year, variable, destination)

        destination = context.destination
        if not os.path.isdir(destination):
            os.makedirs(destination)

        result = self.destination_file_name(context, year, variable)

        self.compute_tasks = []
        if Shape.polygon in context.shapes or not context.points:
            self.compute_tasks = [
                ComputeShapesTask(year,
                                  variable,
                                  self.download_task.target(),
                                  result,
                                  context.strategy,
                                  shape_file,
                                  context.geography)
                for shape_file in [
                    self.find_shape_file(context, year, shape)
                    for shape in context.shapes
                ]
            ]
        if Shape.point in context.shapes and context.points:
            self.compute_tasks += [
                ComputePointsTask(year,
                                  variable,
                                  self.download_task.target(),
                                  result,
                                  context.points,
                                  context.coordinates,
                                  context.metadata)
            ]
        if not self.compute_tasks:
            raise Exception("Invalid combination of arguments")


    def execute(self):
        """
        Executes the task. First the download subtask is executed unless
        the corresponding file has already been downloaded. Then the compute
        tasks are executed

        :return: None
        """

        self.download_task.execute()
        for task in self.compute_tasks:
            task.execute()


