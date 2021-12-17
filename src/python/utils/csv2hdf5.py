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

import csv
import os

import h5py
import pandas
from nsaph_utils.utils.io_utils import fopen


def to_data_frame(path: str) -> pandas.DataFrame:
    with fopen(path, "rb") as stream:
        reader = csv.reader(stream)
        df = pandas.DataFrame(reader)
        return df


def to_hdf5(df: pandas.DataFrame, path: str):
    name = os.path.basename(path)
    with h5py.File(path, "w") as f:
        f.create_dataset(name, data=df)


def transfer(path: str):
    name = os.path.basename(path).split('.')[0]
    d = os.path.dirname(path)
    fp = os.path.join(d, name + ".hdf5")

    with fopen(path, "rb") as stream, h5py.File(fp, "w") as f:
        reader = csv.reader(stream)
        header = next(reader)
        ds = f.create_dataset(name, a)
        for row in reader:
            pass



if __name__ == '__main__':
    data_frame = to_data_frame("data/processed/rmax_custom_point_2016.csv.gz")
    print(data_frame.head(10))

