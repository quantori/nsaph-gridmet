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
library(lubridate)
library(ncdf4)
library(raster)
library(sf)
library(data.table)
library(velox)

ellen <- function(rast, variable) {
  raw_points <- fread("data/ustemp_wgs84.csv.gz")
  points <- st_as_sf(raw_points, coords = c("xcoord", "ycoord"))
  geoid.in <- "SiteCode"
  geoid.out <- "site_code"
  return (process_year_point(rast, points, variable, geoid.in, geoid.out, 10))
}

zip <- function(rast, variable) {
  raw_points <- fread("data/ESRI01USZIP5_POINT_WGS84_POBOX.csv.gz")
  points <- st_as_sf(raw_points, coords = c("POINT_X", "POINT_Y"))
  geoid.in <- "ZIP"
  geoid.out <- "zip"
  return (process_year_point(rast, points, variable, geoid.in, geoid.out, N=20))
}

test <- function() {
  f <- "data/downloads/tmmx_2001.nc"
  variable <- "tmmx"
  varname <- "air_temperature"
  rast <- brick(f, varname = varname)
  #year_data <- zip(rast, variable)
  #out <- "rtst_zip.csv"
  year_data <- ellen(rast, variable)
  out <- "rtst_ellen.csv"
  fwrite(year_data, file.path("data/temp", out))
}

process_year_point <- function(rast, point, variable, geoid.in, geoid.out, N=0) {
  out <- NULL
  if (N == 0) {
    N <- nlayers(rast)
  }
  cat(now())
  for (i in 1:N) {
    temp_layer <- rast[[i]]
    date <- as.Date(temp_layer@z[[1]], origin = "1900-01-01")
    cat(paste("processing",date, "\n"))
    temp_df <- data.table(velox(temp_layer)$extract_points(point)[,1])
    names(temp_df) <- variable
    temp_df[, date := date]
    temp_df[, (geoid.out) := point[[geoid.in]]]
    out <- rbind(out, temp_df)
  }
  cat(now())

  return(out)
}

setwd("/Users/misha/harvard/projects/gis")
test()