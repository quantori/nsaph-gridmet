import csv
import gzip
from datetime import datetime

import geopandas
import pandas
from shapely.geometry import Point

from gridmet_tools import find_shape_file


def get_zips_dataframe(path_to_dir:str, year: int) -> geopandas.GeoDataFrame:
    shapefile = find_shape_file(path_to_dir, year, "zip", "polygon")
    return geopandas.GeoDataFrame.from_file(shapefile)


def annotate(input_stream, output_stream, zips, columns,
             nrows = 100000, crs="EPSG:4326") -> int:
    if len(columns) > 0:
        original = pandas.read_csv(input_stream, nrows=nrows, names=columns)
    else:
        original = pandas.read_csv(input_stream, nrows=nrows, header=0)
    actual_n_rows =len(original)
    if actual_n_rows < 1:
        return actual_n_rows
    if not columns:
        columns.extend(original.columns)
    geometry = [Point(xy) for xy in zip(original.xcoord, original.ycoord)]
    points = geopandas.GeoDataFrame(original, geometry=geometry, crs=crs)
    points_to_zip = geopandas.sjoin(points, zips, how='left').rename(columns={
        "PO_NAME": "NAME"})
    joined = points_to_zip[list(columns) + ["ZIP", "STATE", "NAME"]]
    joined.to_csv(output_stream, index=False)
    return actual_n_rows


if __name__ == '__main__':
    zips = get_zips_dataframe("shapes/zip_shape_files/", 2016)
    original_columns = []
    with gzip.open("data/ustemp_wgs84.csv.gz", "rt") as point_file, \
        gzip.open("data/ustemp_wgs84_transformed.csv.gz", "w") as output:
        t0 = datetime.now()
        n = 0
        step = 0
        while True:
            step += 1
            t1 = datetime.now()
            nrows = annotate(point_file, output, zips, columns=original_columns)
            if nrows < 1:
                break
            n += nrows
            t2 = datetime.now()
            t = t2 - t0
            rate = (t2 - t1) / nrows * 1000000
            print("{:d} \t{} [{}]".
                  format(step, str(rate), str(t)))
    t = datetime.now() - t0
    print("Completed in {}".format(str(t)))



