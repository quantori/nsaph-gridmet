#  Copyright (c) 2021. Harvard University
#
#  Developed by Research Software Engineering,
#  Faculty of Arts and Sciences, Research Computing (FAS RC)
#  Author: Michael A Bouzinier
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import datetime
from enum import Enum
from typing import Optional

from nsaph_gis.constants import Geography, RasterizationStrategy
from nsaph_utils.utils.context import Context, Argument, Cardinality

var_doc_string = """
        Gridmet bands or variables. 
        :ref: `doc/bands`
"""


class DateFilter:
    def __init__(self, value: str):
        self.min = None
        self.max = None
        self.ftype = None
        self.values = []
        if not value:
            return
        if ':' not in value:
            raise ValueError("Filter spec must include ':'")
        bounds = value.split(':')
        if bounds[0].lower() in ["dayofmonth", "month", "date"]:
            self.ftype = bounds[0].lower()
            self.values = [v.strip() for v in bounds[1].split(',')]
        else:
            self.ftype = "range"
            self.min = datetime.date.fromisoformat(bounds[0])
            self.max = datetime.date.fromisoformat(bounds[1])

    def accept(self, day: datetime.date):
        if self.ftype == "dayofmonth":
            dom = str(day.day)
            if dom in self.values:
                return True
            return False
        elif self.ftype == "month":
            mnth = str(day.month)
            if mnth in self.values:
                return True
            return False
        elif self.ftype == "date":
            dt = day.strftime("%m-%d")
            if dt in self.values:
                return True
            if dt.strip('0') in self.values:
                return True
            return False
        if self.min and day < self.min:
            return False
        if self.max and day > self.max:
            return False
        return True


class Shape(Enum):
    """Type of shape"""

    point = "point"
    """Point"""
    polygon = "polygon"
    """Polygon"""


class GridmetVariable(Enum):
    """
    `Gridmet Bands <https://gee.stac.cloud/WUtw2spmec7AM9rk6xMXUtStkMtbviDtHK?t=bands>`
    """
    bi = "bi"
    """Burning index: NFDRS fire danger index"""
    erc = "erc"
    """Energy release component: NFDRS fire danger index"""
    etr = "etr"
    """Daily reference evapotranspiration: Alfalfa, mm"""
    fm100 = "fm100"
    """100-hour dead fuel moisture: %"""
    fm1000 = "fm1000"
    """1000-hour dead fuel moisture: %"""
    pet = "pet"
    """Potential evapotranspiration"""
    pr = "pr"
    """Precipitation amount: mm, daily total """
    rmax = "rmax"
    """Maximum relative humidity: %"""
    rmin = "rmin"
    """Minimum relative humidity: %"""
    sph = "sph"
    """Specific humididy: kg/kg"""
    srad = "srad"
    """Surface downward shortwave radiation: W/m^2"""
    th = "th"
    """Wind direction: Degrees clockwise from North"""
    tmmn = "tmmn"
    """Minimum temperature: K"""
    tmmx = "tmmx"
    """Maximum temperature: K"""
    vpd = "vpd"
    """Mean vapor pressure deficit: kPa"""
    vs = "vs"
    """Wind velocity at 10m: m/s"""


class GridmetContext(Context):
    """
    Defines a context for running a gridmet pipeline.
    """
    _variables = Argument("variables",
                          help="Gridmet bands or variables",
                          aliases=["var"],
                          cardinality=Cardinality.multiple,
                          valid_values=[v.value for v in GridmetVariable])
    _strategy = Argument("strategy",
                         aliases=['s'],
                         default=RasterizationStrategy.default.value,
                         help="Rasterization Strategy",
                         valid_values=[v.value for v in RasterizationStrategy])
    _destination = Argument("destination",
                            aliases=['dest', 'd'],
                            cardinality=Cardinality.single,
                            default="data/processed",
                            help="Destination directory for the processed files"
                            )
    _raw_downloads = Argument("raw_downloads",
                              cardinality=Cardinality.single,
                              default="data/downloads",
                              help="Directory for downloaded raw files"
                            )
    _geography = Argument("geography",
                          cardinality = Cardinality.single,
                          default = "zip",
                          help = "The type of geographic area over "
                                 + "which we aggregate data",
                          valid_values=[v.value for v in Geography]
                          )
    _shapes_dir = Argument("shapes_dir",
                           default="shapes",
                           help="Directory containing shape files for"
                            + " geographies. Directory structure is"
                            + " expected to be: "
                            + ".../${year}/${geo_type}/{point|polygon}/")
    _shapes = Argument("shapes",
                       cardinality=Cardinality.multiple,
                       default=[Shape.polygon.value],
                       help="Type of shapes to aggregate over",
                       valid_values=[v.value for v in Shape]
                       )
    _points = Argument("points",
                       cardinality=Cardinality.single,
                       default="",
                       help="Path to CSV file containing points")
    _coordinates = Argument("coordinates",
                            aliases=["xy", "coord"],
                            cardinality=Cardinality.multiple,
                            default="",
                            help="Column names for coordinates")
    _metadata = Argument("metadata",
                            aliases=["m", "meta"],
                            cardinality=Cardinality.multiple,
                            default="",
                            help="Column names for metadata")
    _dates = Argument("dates",
                      help="Filter dates - for debugging purposes only",
                      required=False)
    _shape_files = Argument("shape_files",
                       cardinality=Cardinality.multiple,
                       default="",
                       help="Path to shape files",
                       )

    def __init__(self, doc = None):
        """
        Constructor
        :param doc: Optional argument, specifying what to print as documentation
        """

        self.variables = None
        """
        Gridmet bands or variables 
        
        :type: GridmetVariable 
        """

        self.strategy = None
        """
        Rasterization strategy
        :type: RasterizationStrategy
        """

        self.destination = None
        '''Destination directory for the processed files'''
        self.raw_downloads = None
        '''Directory for downloaded raw files'''
        self.geography = None
        """
        The type of geographic area over which we aggregate data
        
        :type: Geography
        """

        self.shapes_dir = None
        '''Directory containing shape files for geographies'''
        self.shapes = None
        """
        Type of shapes to aggregate over, e.g. points, polygons
        
        :type: Shape
        """
        self.shape_files = None

        self.points = None
        '''Path to CSV file containing points'''
        self.coordinates = None
        '''Column names for coordinates'''
        self.metadata = None
        '''Column names for metadata'''
        self.dates: Optional[DateFilter] = None
        '''Filter on dates - for debugging purposes only'''
        super().__init__(GridmetContext, doc)

    def validate(self, attr, value):
        value = super().validate(attr, value)
        if attr == self._variables.name:
            return [GridmetVariable(v) for v in value]
        if attr == self._shapes.name:
            return [Shape(v) for v in value]
        if attr == self._geography.name:
            return Geography[value]
        if attr == self._strategy.name:
            return RasterizationStrategy[value]
        if attr == self._dates.name:
            if value:
                return DateFilter(value)
        return value
