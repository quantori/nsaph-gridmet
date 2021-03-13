from enum import IntEnum, Enum
from nsaph_utils.utils.context import Context, Argument, Cardinality




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
    _years = Argument("years",
                     aliases=['y'],
                     cardinality=Cardinality.multiple,
                     default="1990:2020",
                     help="Year or list of years to download. For example, " +
                        "the following argument: " +
                        "`-y 1992:1995 1998 1999 2011 2015:2017` will produce " +
                        "the following list: " +
                        "[1992,1993,1994,1995,1998,1999,2011,2015,2016,2017]"
                     )
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

    def __init__(self, doc = None):
        """
        Constructor
        :param doc: Optional argument, specifying what to print as documentation
        """
        self.years = None
        self.variables = None
        self.strategy = None
        self.destination = None
        self.raw_downloads = None
        super().__init__(GridmetContext, doc)
