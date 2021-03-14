from enum import IntEnum, Enum
from nsaph_utils.utils.context import Context, Argument, Cardinality


class Geography(Enum):
    zip = "zip"
    county = "county"


class Shape(Enum):
    point = "point"
    polygon = "polygon"


class RasterizationStrategy(Enum):
    DEFAULT = "default"
    ALL_TOUCHED = "all_touched"
    COMBINED = "combined"


class GridmetVariable(Enum):
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
    _variables = Argument("variable",
                          aliases=["var"],
                          cardinality=Cardinality.multiple,
                          help="Gridmet variable",
                          valid_values=[v.value for v in GridmetVariable])
    _strategy = Argument("strategy",
                         aliases=['s'],
                         default=RasterizationStrategy.DEFAULT,
                         help="Rasterization Strategy",
                         valid_values=[v.value for v in RasterizationStrategy])
    _destination = Argument("destination",
                            aliases=['dest', 'd'],
                            cardinality=Cardinality.single,
                            default="data/processed",
                            help="Destination directory for the processed files"
                            )
    _raw_downloads = Argument("downloads",
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
                       default=Shape.polygon.value,
                       help="Type of shapes to aggregate over")

    def __init__(self, doc = None):
        """
        Constructor
        :param doc: Optional argument, specifying what to print as documentation
        """
        self.variables = None
        self.strategy = None
        self.destination = None
        self.raw_downloads = None
        self.geography = None
        self.shapes_dir = None
        self.shapes = None
        super().__init__(GridmetContext, doc)
