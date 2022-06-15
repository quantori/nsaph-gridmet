"""
Python module to download EPA AirNow Data using WebServices API

https://docs.airnowapi.org/webservices

AirNow contains real-time up-to-date pollution data but is less reliable
than AQS

"""
#  Copyright (c) 2021-2022. Harvard University
#
#  Developed by Research Software Engineering,
#  Faculty of Arts and Sciences, Research Computing (FAS RC)
#  in collaboration with Quantori LLC
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

import argparse

from nsaph_gis.constants import Geography
from nsaph_gis.downloader import GISDownloader


def download_shapes(year, geography):
    if geography == Geography.county.value:
        GISDownloader.download_county(year)
    elif geography == Geography.zip.value:
        GISDownloader.download_zip(year)
    else:
        raise ValueError("Unknown geography: " + geography)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", "-y", dest="year", type=int, required=True, help="Year")
    ap.add_argument("--geography", "--geo", "-g", required=True,
                    help="One of: " + ", ".join([
                        v.value for v in Geography
                    ])
                    )
    args = ap.parse_args()

    download_shapes(year=args.year, geography=args.geography)
