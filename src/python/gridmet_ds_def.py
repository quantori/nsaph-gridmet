from enum import IntEnum, Enum
from nsaph_utils.utils.context import Context, Argument, Cardinality

var_doc_string = """
        Gridmet bands or variables. 
        :ref: `doc/bands`
"""

class Geography(Enum):
    """Type of geography"""

    zip = "zip"
    """Zip Code Area"""
    county = "county"
    """County"""
    custom = "custom"
    """User custom"""


class Shape(Enum):
    """Type of shape"""

    point = "point"
    """Point"""
    polygon = "polygon"
    """Polygon"""


class RasterizationStrategy(Enum):
    """
    Rasterization Strategy, see details at
    https://pythonhosted.org/rasterstats/manual.html#rasterization-strategy
    """
    default = "default"
    """
    The default strategy is to include all pixels along the line render path
    (for lines), or cells where the center point is within the polygon
    (for polygons). 
    """
    all_touched = "all_touched"
    """
    Alternate, all_touched strategy, rasterizes the geometry
    by including all pixels that it touches.
    """
    combined = "combined"
    """
    Calculate statistics using both default and all_touched strategy and
    combine results, e.g. using arithmetic means 
    """
    downscale = "downscale"
    """Use disaggregate with factor = 5"""


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
                                 + "which we aggregate data"
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
                       help="Type of shapes to aggregate over")
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

    def __init__(self, doc = None):
        """
        Constructor
        :param doc: Optional argument, specifying what to print as documentation
        """

        self.variables = None
        '''Gridmet bands or variables :type: `GridmetVariable` '''
        self.strategy = None
        '''Rasterization strategy'''
        self.destination = None
        '''Destination directory for the processed files'''
        self.raw_downloads = None
        '''Directory for downloaded raw files'''
        self.geography = None
        '''The type of geographic area over which we aggregate data'''
        self.shapes_dir = None
        '''Directory containing shape files for geographies'''
        self.shapes = None
        '''Type of shapes to aggregate over, e.g. points, polygons'''
        self.points = None
        '''Path to CSV file containing points'''
        self.coordinates = None
        '''Column names for coordinates'''
        self.metadata = None
        '''Column names for metadata'''
        super().__init__(GridmetContext, doc)
        return

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
        return value

