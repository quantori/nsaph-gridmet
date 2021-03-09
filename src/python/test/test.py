import netCDF4 as nc
import random
from rasterstats import zonal_stats, point_query


if __name__ == '__main__':
    fn = '/Users/misha/harvard/projects/data_server/nsaph/local_data/V4NA03_PM25_NA_200001_200012-RH35.nc'
    ds = nc.Dataset(fn)
    print(ds)

    pm25 = ds["PM25"]
    lat = ds["LAT"]
    lon = ds["LON"]
    random.seed(0)
    for i in range(0, 20):
        lo = random.randrange(0, len(lon))
        la = random.randrange(0, len(lat))
        p = pm25[la, lo]
        print("[{:d},{:d}]: ({:f}, {:f}) pm25={:f}".format(lo, la, lat[la], lon[lo], p))

    shape = "/Users/misha/harvard/projects/gis/shapes/zip_shape_files/2017/zip/point/ESRI17USZIP5_POINT_WGS84.shp"
    stats = zonal_stats(shape, fn)
    print(stats)
    pts = point_query(shape, fn)
    print(pts)