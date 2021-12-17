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

import os
import sys
from pathlib import Path
import yaml

from nsaph import init_logging
from nsaph.pg_keywords import PG_NUMERIC_TYPE, PG_DATE_TYPE, PG_INT_TYPE

from gridmet.config import Geography, GridmetVariable


class Registry:
    """
    This class parses File Transfer Summary files and
    creates YAML data model. It can either
    update built-in registry or write
    the model to a designated path
    """

    def __init__(self, destination:str):
        self.destination = destination
        init_logging()

    def update(self):
        with open(self.destination, "wt") as f:
            f.write(self.create_yaml())
        return

    def create_yaml(self):
        name = "gridmet"
        domain = {
            name: {
                "schema": name,
                "index": "all",
                "description": "NSAPH data model for gridMET",
                "header": True,
                "quoting": 3,
                "tables": {
                }
            }
        }
        for geography in Geography:
            for band in GridmetVariable:
                bnd = band.value
                geo = geography.value
                date_column = "observation_date"
                tname = "{}_{}".format(geo, bnd)
                table = {
                    "columns": [
                        {bnd: {
                            "type": PG_NUMERIC_TYPE
                        }},
                        {date_column: {
                            "type": PG_DATE_TYPE,
                            "source": "date"
                        }},
                        {geo: {
                            "type": PG_INT_TYPE
                        }}
                    ],
                    "primary_key": [
                        geo,
                        date_column
                    ],
                    "indices": {
                        "dt_geo_idx": {
                            "columns": [date_column, geo]
                        }
                    }


                }
                domain[name]["tables"][tname] = table

        return yaml.dump(domain)

    @staticmethod
    def built_in_registry_path():
        src = Path(__file__).parents[3]
        return os.path.join(src, "yml", "cms.yaml")


if __name__ == '__main__':
    Registry(sys.argv[1]).update()
    