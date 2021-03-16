from enum import IntEnum, Enum
from nsaph_utils.utils.context import Context, Argument, Cardinality


class Geography(Enum):
    zip = "zip"
    """Zip Code Area"""
    county = "county"
    """County"""


class Shape(Enum):
    point = "point"
    """Point"""
    polygon = "polygon"
    """Polygon"""


class RasterizationStrategy(Enum):
    """
    Rasterization Strategy, see details at
    https://pythonhosted.org/rasterstats/manual.html#rasterization-strategy
    The default strategy is to include all pixels along the line render path
    (for lines), or cells where the center point is within the polygon
    (for polygons). Alternate, all_touched strategy, rasterizes the geometry
    by including all pixels that it touches.
    """
    default = "default"
    all_touched = "all_touched"
    combined = "combined"


class GridmetVariable(Enum):
    """
    Climate variables available at
    https://www.northwestknowledge.net/metdata/data/
    """
    bi = "bi"
    erc = "erc"
    etr = "etr"
    fm100 = "fm100"
    fm1000 = "fm1000"
    pet = "pet"
    pr = "pr"
    rmax = "rmax"
    rmin = "rmin"
    sph = "sph"
    srad = "srad"
    th = "th"
    tmmn = "tmmn"
    tmmx = "tmmx"
    vpd = "vpd"
    vs = "vs"


class GridmetContext(Context):
    _variables = Argument("variables",
                          aliases=["var"],
                          cardinality=Cardinality.multiple,
                          help="Gridmet variable",
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
                           help="Directory containing shape files for geographies."
                            + " Directory structure is"
                            + " expected to be: "
                            + ".../${year}/${geo_type}/{point|polygon}/")
    _shapes = Argument("shapes",
                       cardinality=Cardinality.multiple,
                       default=[Shape.polygon.value],
                       help="Type of shapes to aggregate over")

    def __init__(self, doc = None):
        """
        Constructor
        :param doc: Optional argument, specifying what to print as documentation
        """
        self.variables = None
        """Gridmet variables"""
        self.strategy = None
        """Rasterization strategy"""
        self.destination = None
        self.raw_downloads = None
        self.geography = None
        self.shapes_dir = None
        self.shapes = None
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
        return value

