import csv
import math
import sys
from datetime import datetime
from typing import List, Dict, Set, Optional

import geopandas
import pandas
from nsaph_utils.utils.io_utils import fopen
from shapely.geometry import Point

from gridmet.gridmet_tools import find_shape_file


def get_zips_dataframe(path_to_dir:str, year: int) -> geopandas.GeoDataFrame:
    shapefile = find_shape_file(path_to_dir, year, "zip", "polygon")
    return geopandas.GeoDataFrame.from_file(shapefile)


def df_row2dict(row: List, columns: List, to_quote: Set) -> Dict:
    d = {}
    for i in range(0, len(columns)):
        key = columns[i]
        value = row[i]
        if isinstance(value, float) and math.isnan(value):
            value = None
        if to_quote and key in to_quote:
            value = '"{}"'.format(str(value))
        d[key] = value
    return d


def join_with_shapes(buffer: List, shapes: geopandas.GeoDataFrame,
                     original_columns: List, shape_columns: List,
                     crs="EPSG:4326",
                     columns_to_quote: Set = None) -> Optional[List[dict]]:
    original = pandas.DataFrame(data=buffer, columns=original_columns)
    actual_n_rows =len(original)
    if actual_n_rows < 1:
        return None
    geometry = [Point(xy) for xy in zip(original.xcoord, original.ycoord)]
    points = geopandas.GeoDataFrame(original, geometry=geometry, crs=crs)
    points_in_shapes = geopandas.sjoin(points, shapes, how='left')\
        .rename(columns={"PO_NAME": "NAME"})

    all_columns = original_columns + shape_columns
    joined = points_in_shapes[all_columns]
    rows = [
        df_row2dict(row, all_columns, columns_to_quote) for row in joined.values
    ]

    return rows


class ZipAnnotator:
    SHAPE_COLUMNS = ["ZIP", "STATE", "NAME"]

    def __init__(self, year: int, source: str, destination: str,
                 latitude: str, longitude: str, shapes_dir: str,
                 to_quote: str = None,
                 buffer_size: int = 100000, shape_columns=None):
        self.zips = get_zips_dataframe(shapes_dir, year)
        self.src = source
        self.dest = destination
        self.latitude = latitude
        self.longitude = longitude
        if to_quote:
            self.to_quote = {s for s in to_quote.split(',')}
        else:
            self.to_quote = None
        self.buf_size = buffer_size
        if shape_columns is None:
            self.shape_columns = self.SHAPE_COLUMNS
        else:
            self.shape_columns = shape_columns
        self.line = 0
        self.step = 0
        self.t0 = None
        self.t1 = None
        self.columns = None
        self.writer = None
        self.output = None

    def annotate(self):
        self.t0 = datetime.now()
        with fopen(self.src, "r") as point_file, \
                fopen(self.dest, "w") as self.output:
            reader = csv.DictReader(point_file)
            self.columns = reader.fieldnames
            self.writer = csv.DictWriter(self.output, quoting=csv.QUOTE_NONE,
                                         quotechar='',
                                         fieldnames=self.columns +
                                                    self.shape_columns)
            self.writer.writeheader()
            self.t1 = self.t0
            buffer = []
            for row in reader:
                for i in [self.latitude, self.longitude]:
                    row[i] = float(row[i])
                buffer.append(row)
                self.line += 1
                if (self.line % self.buf_size) == 0:
                    self.flush(buffer)
                    buffer.clear()
            if len(buffer) > 0:
                self.flush(buffer)
        t = datetime.now() - self.t0
        print("Completed in {}".format(str(t)))

    def flush(self, buffer: List):
        self.step += 1
        buffer = join_with_shapes(buffer, self.zips, self.columns,
                                  self.shape_columns,
                                  columns_to_quote=self.to_quote)
        self.writer.writerows(buffer)
        self.output.flush()
        t2 = datetime.now()
        t = t2 - self.t0
        rate = (t2 - self.t1) / len(buffer) * 1000000
        print("{:d} \t{} [{}]".
              format(self.step, str(rate), str(t)))
        self.t1 = datetime.now()
        return


if __name__ == '__main__':
    annotator = ZipAnnotator(2016, sys.argv[1], sys.argv[2],
                             latitude="ycoord", longitude="xcoord",
                             to_quote="SiteCode",
                             shapes_dir="shapes/zip_shape_files")
    annotator.annotate()



