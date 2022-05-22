#!/usr/bin/env cwl-runner
### Downloader of gridMET Data
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

cwlVersion: v1.2
class: CommandLineTool
baseCommand: [python, -m, gridmet.launcher]

requirements:
  InlineJavascriptRequirement: {}
  EnvVarRequirement:
    envDef:
      HTTP_PROXY: "$('proxy' in inputs? inputs.proxy: null)"
      HTTPS_PROXY: "$('proxy' in inputs? inputs.proxy: null)"
      NO_PROXY: "localhost,127.0.0.1,172.17.0.1"
  ResourceRequirement:
    # coresMin: 1
    coresMax: 2


doc: |
  This tool preprocesses gridMET to aggregate over shapes
  (zip codes or counties) and time. It produces daily mean values

inputs:
  proxy:
    type: string?
    default: ""
    doc: HTTP/HTTPS Proxy if required
  strategy:
    type: string
    default: downscale
    inputBinding:
      prefix: --strategy
    doc: "Rasterization strategy"
  shapes:
    type: Directory?
    inputBinding:
      prefix: --shapes_dir
  geography:
    type: string
    doc: |
      Type of geography: zip codes or counties
    inputBinding:
      prefix: --geography
  year:
    type: string
    doc: "Year to process"
    inputBinding:
      prefix: --years
  band:
    type: string
    doc: |
      [Gridmet Band](https://gee.stac.cloud/WUtw2spmec7AM9rk6xMXUtStkMtbviDtHK?t=bands)
    inputBinding:
      prefix: --var
  dates:
    type: string?
    doc: 'dates restriction, for testing purposes only'
    inputBinding:
      prefix: --dates
  input:
    type: File
    doc: "Downloaded file"
  shape_files:
    type: File[]
    doc: "Paths to shape files"
    inputBinding:
      prefix: --shape_files

arguments:
  - valueFrom: $(inputs.band)
    prefix: --destination
  - valueFrom: |
      ${
          return inputs.input["dirname"];
      }
    prefix: --raw_downloads

outputs:
  log:
    type: File?
    outputBinding:
      glob: "*.log"
  data:
    type: File?
    doc: |
      The output CSV file, containing daily means of the given
      gridMET variable over given geographies. Each line
      contains date, geo id (zip or county FIPS) and value
    outputBinding:
      glob: $(inputs.band + "/*.csv.gz")
  errors:
    type: stderr

stderr: $("aggr-" + inputs.band + "-" + inputs.year + ".err")
