#!/usr/bin/env cwl-runner
### Downloader of gridMET Data

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


doc: |
  This tool downloads gridMET data from Atmospheric Composition Analysis Group
  and then preprocesses it to aggregate over shapes (zip codes or counties)

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
    type: Directory
    inputBinding:
      prefix: --shapes_dir
  geography:
    type: string
    doc: |
      Type of geography: zip codes or counties
    inputBinding:
      prefix: --geography
  years:
    type: string
    doc: "Years to process"
    default: "1990:2021"
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


outputs:
  log:
    type: File?
    outputBinding:
      glob: "*.log"
  data:
    type: File[]
    outputBinding:
      glob: "*/*.csv.gz"
  errors:
    type: stderr

stderr: download.err
