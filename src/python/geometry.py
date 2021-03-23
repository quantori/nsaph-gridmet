import itertools

from rasterstats import point, point_query
from numpy.ma import masked


class PointInRaster:
    COMPLETELY_MASKED = 1
    PARTIALLY_MASKED = 2

    def __init__(self, raster, affine, x, y):
        self.window, unitxy = point.point_window_unitxy(x, y, affine)
        self.x, self.y = unitxy
        self.masked = 0

        m = 0
        array = raster.read(window=self.window, masked=True).array
        for i, j in itertools.product([0,1], [0,1]):
            r = self.window[0][0] + i
            c = self.window[1][0] + j
            if array[i, j] is masked:
                m += 1
            elif raster.array[r, c] is masked:
                m += 1
            else:
                self.r, self.c = r, c
        if m == 4:
            self.masked = self.COMPLETELY_MASKED
        elif m > 0:
            self.masked = self.PARTIALLY_MASKED
        return

    def is_masked(self):
        return self.masked == self.COMPLETELY_MASKED

    def bilinear(self, raster) -> float:
        if self.masked == self.COMPLETELY_MASKED:
            return None
        if self.masked == self.PARTIALLY_MASKED:
            return raster.array[self.r, self.c]
        array = raster.read(window=self.window, masked=True).array
        return point.bilinear(array, self.x, self.y)
