import csv
import os
import sys
from typing import List

import netCDF4 as nc
from datetime import date, timedelta, datetime
#from affine import Affine
from rasterstats import zonal_stats
import rasterio
import shapefile

from nsaph_utils.utils.io_utils import DownloadTask, as_stream, fopen


def download_data(task: DownloadTask):
    print(str(task))
    size = 65536
    buffer = bytearray(size)
    with fopen(task.destination, "wb") as writer, as_stream(task.urls[0]) as reader:
        n = 0
        while True:
            ret = reader.readinto(buffer)
            if not ret:
                break
            writer.write(buffer[:ret])
            n += 1
            if (n % 20) == 0:
                print("*", end = '')
    return


def get_atmos_url(year:int, variable ="PM25") -> str:
    base = "http://fizz.phys.dal.ca/~atmos/datasets/V4NA03/"
    pattern = base + "V4NA03_{}_NA_{:d}01_{:d}12-RH35.nc"
    return pattern.format(variable, year, year)


def get_nw_url(year:int, variable) -> str:
    base = "https://www.northwestknowledge.net/metdata/data/"
    pattern = base + "{}_{:d}.nc"
    return pattern.format(variable, year)


def get_shape_file_metadata(shape_file:str) -> List:
    with shapefile.Reader(shape_file) as shape:
        print(shape)
        shapes = [(r.STATE, r.ZIP) for r in shape.records()]
    return shapes


def find_shape_file(parent_dir: str, year: int):
    shape_file = None
    y = year
    while y > 1999:
        d = os.path.join(parent_dir, "{:d}".format(y))
        if os.path.isdir(d):
            f = "zip/polygon/ESRI{:02d}USZIP5_POLY_WGS84.shp".format(y - 2000)
            shape_file = os.path.join(d, f)
            break
        y -= 1
    if shape_file is None:
        raise Exception(
            "Could not find ZIP shape file for year {:d} or earlier"
                .format(year))
    return shape_file


if __name__ == '__main__':
    year = int(sys.argv[1])
    variable = "tmmx"
    url = get_nw_url(year=year, variable=variable)
    f = url.split('/')[-1]
    d = "data/downloads"
    if not os.path.isdir(d):
        os.makedirs(d)
    target = os.path.join(d, f)

    task = DownloadTask(target, [url])
    if not task.is_up_to_date():
        download_data(task)

    ds = nc.Dataset(target)
    with rasterio.open(target) as rio:
        affine = rio.transform
    for dim in ds.dimensions.values():
        print(dim)
    for var in ds.variables.values():
        try:
            description = var.description
        except:
            description = var.long_name
        print("{}: {}".format(var.name, description))

    origin = date(1900, 1, 1)
    shape_file = find_shape_file("shapes/zip_shape_files", year)

    days = ds["day"][:]
    d = "data/processed"
    f = "{}_zip_{:d}.csv".format(variable, year)
    if not os.path.isdir(d):
        os.makedirs(d)
    t0 = datetime.now()
    with open(os.path.join(d, f), "w") as out:
        writer = csv.writer(out, delimiter=',', quoting=csv.QUOTE_NONE)
        writer.writerow([variable, "date", "zip"])
        for idx in range(0, len(days)):
            day = days[idx]
            dt = origin + timedelta(days=day)
            layer = ds["air_temperature"][idx, :, :]
            print(dt, end='')
            t1 = datetime.now()
            stats1 = zonal_stats(shape_file, layer, stats ="mean",
                                 affine=affine, geojson_out=True,
                                 all_touched=False)
            t2 = datetime.now()
            stats2 = zonal_stats(shape_file, layer, stats ="mean",
                                 affine=affine, geojson_out=True,
                                 all_touched=True)
            t3 = datetime.now()
            for i in range(0, len(stats1)):
                r1 = stats1[i]
                r2 = stats2[i]
                zip = r1['properties']['ZIP']
                assert zip == r2['properties']['ZIP']
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
                #print (record)
                # print("{} {}: {}".format(record['properties']['STATE'],
                #                          record['properties']['ZIP'],
                #                          record['properties']['mean']))
                writer.writerow([mean, dt.strftime("%Y-%m-%d"), zip])
            t = datetime.now() - t0
            fmt = "%H:%M:%S.%f"
            print(" \t{}/{} [{}]".format(str(t2 - t1), str(t3 - t2), str(t)))





