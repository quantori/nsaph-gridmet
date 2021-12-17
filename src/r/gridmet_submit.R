#  Copyright (c) 2021. Harvard University
#
#  Developed by Research Software Engineering,
#  Faculty of Arts and Sciences, Research Computing (FAS RC)
#  Author: Ben Sabath
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
## Code to download temperature and relative humidity data for 2001 and 2002

library(sf)
library(data.table)
library(NSAPHclimate)
library(optparse)
library(fs)

option_list <- list(
  make_option(c("-y", "--year"), type="character", default=NULL,
              help="Year", metavar="character", ),
  make_option(c("-p", "--point"), type="character", default=NULL,
              help="Path to points file", metavar="character", ),
  make_option(c("-s", "--shape"), type="character", default=NULL,
              help="Path to shape file", metavar="character"),
  make_option(c("-v", "--var"), type="character", default=NULL,
              help="Variable to aggregate", metavar="character"),
  make_option(c("-o", "--out"), type="character", default="out.txt",
              help="output directory [default= %default]", metavar="character"),
  make_option(c("-t", "--test"), action="store_true", default = FALSE,
              help="do not run actual compute", metavar="character")
);

opt_parser <- OptionParser(option_list=option_list);
opt <- parse_args(opt_parser);

year <- opt$year;
var <- opt$var;
shape<- opt$shape;
point <- opt$point;
out_dir <- opt$out;

if (is.null(year) || is.null(var) || is.null(shape) || is.null(point) || is.null(out_dir)) {
  print_help (opt_parser);
  stop(call. = FALSE)
}

temp <- path(out_dir, "temp");
result <- path(out_dir, "results")

print(paste("year: ", year))
print(paste("var: ", var))
print(paste("shape: ", shape))
print(paste("point: ", point))
print(paste("temp: ", temp))
print(paste("result: ", result))

zip_poly <- read_sf(shape);
zip_point <- fread(point);
zip_point <- st_as_sf(zip_point, coords = c("POINT_X", "POINT_Y"));

if (!dir_exists(temp))
  dir_create(temp, recurse=TRUE)
if (!dir_exists(result))
  dir_create(result, recurse=TRUE)

if (opt$test) {
  data <- list(year, var, shape, point, temp, result)
  fwrite (data, paste0(var, "_", "zip", "_", year, ".csv"))
} else {
  get_gridmet(var,
           years = year,
           poly = zip_poly,
           points = zip_point,
           temp_dirname = temp,
           outdir = result)
}





