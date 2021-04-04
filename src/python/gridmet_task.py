import csv
import os
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import Executor, as_completed
from datetime import date, timedelta, datetime
from enum import Enum
from typing import List
from abc import ABC, abstractmethod
import psutil

from netCDF4._netCDF4 import Dataset
from nsaph_utils.utils.io_utils import DownloadTask, fopen, as_stream
from rasterstats import zonal_stats, point_query
from rasterstats.io import Raster
from shapely.geometry import Point

from geometry import PointInRaster
from gridmet_ds_def import RasterizationStrategy, GridmetVariable, \
    GridmetContext, Shape, Geography
from gridmet_tools import find_shape_file, get_nkn_url, get_variable, get_days, \
    get_affine_transform, disaggregate


def count_lines(f):
    with fopen(f, "r") as x:
        return sum(1 for line in x)


class Parallel(Enum):
    points = "points"
    bands = "bands"
    days = "days"


class Collector(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def writerow(self, data: List):
        pass

    def flush(self):
        pass


class CSVWriter(Collector):
    def __init__(self, out_stream):
        super().__init__()
        self.out = out_stream
        self.writer = csv.writer(out_stream,
                                 delimiter=',',
                                 quoting=csv.QUOTE_NONE)

    def writerow(self, row: List):
        self.writer.writerow(row)

    def flush(self):
        self.out.flush()


class ListCollector(Collector):
    def __init__(self):
        super().__init__()
        self.collection = []

    def writerow(self, data: List):
        self.collection.append(data)

    def get_result(self):
        return self.collection


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
        self.band = variable
        self.factor = 1
        self.affine = None
        self.dataset = None
        self.variable = None
        self.parallel = {Parallel.points}

    @classmethod
    def get_variable(cls, dataset: Dataset,  variable: GridmetVariable):
        return get_variable(dataset, variable.value)

    @abstractmethod
    def get_key(self):
        pass

    def prepare(self):
        if not self.affine:
            self.affine = get_affine_transform(self.infile, self.factor)
        print("{} => {}".format(self.infile, self.outfile))
        self.dataset = Dataset(self.infile)
        days = get_days(self.dataset)
        self.variable = self.get_variable(self.dataset, self.band)
        return days

    def execute(self, mode:str = "w"):
        """
        Executes computational task

        :param mode: mode to use opening result file
        :type mode: str
        :return:
        """

        days = self.prepare()
        self.execute_loop(mode, days)

    def execute_loop(self, mode, days):
        with fopen(self.outfile, mode) as out:
            writer = CSVWriter(out)
            if 'a' not in mode:
                writer.writerow([self.band.value, "date", self.get_key().lower()])
            self.collect_data(days, writer)

    def collect_data(self, days: List, collector: Collector):
        t0 = datetime.now()
        for idx in range(0, len(days)):
            day = days[idx]
            layer = self.dataset[self.variable][idx, :, :]
            t1 = datetime.now()
            self.compute_one_day(collector, day, layer)
            collector.flush()
            t3 = datetime.now()
            t = datetime.now() - t0
            print(" \t{} [{}]".format(str(t3 - t1), str(t)))
        return collector

    @abstractmethod
    def compute_one_day(self, writer: Collector, day, layer):
        """
        Computes required statistics for a single day.
        This method is called by `execute()` and is implemented in
        specific subclasses

        :param writer: CSV Writer to output the result
        :param day: day
        :param layer: layer, corresponding to the day
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
        if strategy == RasterizationStrategy.downscale:
            self.strategy = RasterizationStrategy.default
            self.factor = 5
        else:
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


    def compute_one_day(self, writer: Collector, day, layer):
        dt = self.origin + timedelta(days=day)
        if self.factor > 1:
            layer = disaggregate(layer, self.factor)
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

        key = self.get_key()
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

    force_standard_api = False

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
        self.points_file = points_file
        self.points = None
        self.partition = False
        mem = psutil.virtual_memory().free
        file_size = os.stat(self.points_file).st_size
        if self.points_file.lower().endswith(".gz"):
            file_size *= 15
        self.points_in_memory = None
        if self.force_standard_api:
            self.points_in_memory = False
        elif mem > file_size*3:
            n_lines = count_lines(self.points_file)
            if n_lines < 1000000:
                self.points_in_memory = True
        if self.points_in_memory is None:
            self.points_in_memory = False
            self.partition = True
        assert len(coordinates) == 2
        self.coordinates = coordinates
        self.metadata = metadata
        self.step = None
        self.first_layer = None
        self.workers = None
        if Parallel.points in self.parallel:
            self.workers = os.cpu_count() * 2

    def get_key(self):
        return self.metadata[0]

    def execute(self, mode: str = "w"):
        if not self.partition:
            super().execute(mode)
            return
        if self.workers > 1:
            self.execute_parallel()
        else:
            self.execute_sequentially()

    def execute_sequentially(self):
        days = self.prepare()
        with fopen(self.points_file, "r") as points_file:
            reader = csv.DictReader(points_file)
            self.step = 1
            max_len = 500000
            n = 0
            nn = 0
            self.points = []
            mode = "w"
            for row in reader:
                nn += 1
                if self.add_point(row):
                    n += 1
                    if n > max_len:
                        print("Read {:d} points, added to execution queue: {:d}".format(nn, n))
                        self.execute_loop(mode, days)
                        mode = "a"
                        n = 0
                        self.points = []
                        self.step += 1

        print("Read all {:d} points, added to execution queue: {:d}".format(nn, n))
        if len(self.points) > 0:
            self.execute_loop(mode, days)

    def execute_parallel(self):
        days = self.prepare()
        max_len = 50000
        max_tasks = self.workers * 2
        tasks = set()
        with fopen(self.points_file, "r") as points_file, \
                fopen(self.outfile, "w") as out, \
                ThreadPoolExecutor(max_workers=self.workers) as executor:
            reader = csv.DictReader(points_file)
            writer = CSVWriter(out)
            writer.writerow([self.band.value, "date", self.get_key().lower()])
            step = 1
            n = 0
            nn = 0
            points = []
            for row in reader:
                nn += 1
                if self.add_point(row, to=points):
                    n += 1
                    if n > max_len:
                        print(
                            "{:d}: Read {:d} points, added to execution queue: {:d}"
                                .format(step, nn, n))
                        task = self.submit_step(executor, step, days, points)
                        tasks.add(task)
                        n = 0
                        points = []
                        step += 1
                        if (len(tasks) > max_tasks):
                            for completed_task in as_completed(tasks):
                                result = completed_task.result()
                                for rrow in result:
                                    writer.writerow(rrow)
                                writer.flush()
                                tasks.remove(completed_task)
                                break

            print("Read all {:d} points, added to execution queue: {:d}".format(nn, n))
            if len(points) > 0:
                self.submit_step(executor, step, days, points)

            for completed_task in as_completed(tasks):
                result = completed_task.result()
                for rrow in result:
                    writer.writerow(rrow)
                writer.flush()
        return

    def submit_step(self, executor: Executor, step: int,
                    days: List, points: List):
        collector = ListCollector()
        future = executor.submit(self.execute_step,
                                 step, days, points, collector)
        return future

    def execute_step(self, step: int, days: List, points: List,
                     collector: ListCollector):
        t0 = datetime.now()
        tid = threading.get_ident()
        N = 50
        t1 = datetime.now()
        for idx in range(0, len(days)):
            dt = self.origin + timedelta(days=days[idx])
            layer = self.dataset[self.variable][idx, :, :]
            self.compute_one_day_ram(collector, dt, layer, points)
            collector.flush()
            if (idx % N) == N - 1:
                t3 = datetime.now()
                t = datetime.now() - t0
                rate = (t3 - t1) / len(points) / N * 1000000
                print("{:d}|{:d}:{} \t{} [{}]".
                      format(tid, step, str(dt), str(rate), str(t)))
                t1 = datetime.now()
        print("{:d}|{:d}: completed.".format(tid, step))
        return collector.get_result()

    def read_points(self):
        with fopen(self.points_file, "r") as points:
            reader = csv.DictReader(points)
            self.points = []
            for row in reader:
                self.add_point(row)
        return

    def add_point(self, row, to: List = None) -> bool:
        x = float(row[self.coordinates[0]])
        y = float(row[self.coordinates[1]])
        metadata = [row[p] for p in self.metadata]
        point = PointInRaster(self.first_layer, self.affine, x, y)
        if point.is_masked():
            return False
        if to is not None:
            to.append((metadata, point))
        else:
            self.points.append((metadata, point))
        return True

    def prepare(self):
        ret = super().prepare()
        self.first_layer = Raster(self.dataset[self.variable][0, :, :],
                                  self.affine)
        if self.points_in_memory:
            self.read_points()
        return ret

    def compute_one_day(self, writer: Collector, day, layer):
        dt = self.origin + timedelta(days=day)
        if self.step:
            print("{:d}:{}".format(self.step, str(dt)), end='')
        else:
            print(dt, end='')
        date_string = dt.strftime("%Y-%m-%d")
        if self.points_in_memory or self.partition:
            self.compute_one_day_ram(writer, date_string, layer, self.points)
        else:
            self.compute_one_day_file(writer, date_string, layer)
        return

    def compute_one_day_file(self, writer: Collector, date_string, layer):
        with fopen(self.points_file, "r") as points:
            reader = csv.DictReader(points)
            for row in reader:
                x = float (row[self.coordinates[0]])
                y = float (row[self.coordinates[1]])
                metadata = [row[p] for p in self.metadata]
                point = Point(x,y)
                stats = point_query(point, layer, affine=self.affine)
                mean = stats[0]
                writer.writerow([mean, date_string] + metadata)
        return

    def compute_one_day_ram(self, writer: Collector, date_string, layer, points):
        raster = Raster(layer, self.affine)
        for row in points:
            metadata, point = row
            #stats = point_query(point, layer, affine=self.affine)
            mean = point.bilinear(raster)
            writer.writerow([mean, date_string] + metadata)
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
        s = context.shapes[0].value if len(context.shapes) == 1 else "all"
        f = "{}_{}_{}_{:d}.csv".format(variable.value, g, s, year)
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


