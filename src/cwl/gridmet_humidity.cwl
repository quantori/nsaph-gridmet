#!/usr/bin/env cwl-runner
### gridMET Pipeline
#  Copyright (c) 2022. Harvard University
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
class: Workflow

requirements:
  SubworkflowFeatureRequirement: {}
  StepInputExpressionRequirement: {}
  InlineJavascriptRequirement: {}
  ScatterFeatureRequirement: {}
  MultipleInputFeatureRequirement: {}


doc: |
  A subset of gridMET pipeline to retriev humidity data
  Downloads raw data, aggregates it to calculate daily mean values
  for each given geography and ingests it into the database

inputs:
  proxy:
    type: string?
    default: ""
    doc: HTTP/HTTPS Proxy if required
  shapes:
    type: Directory?
    doc: Do we even need this parameter, as we isntead downloading shapes?
  geography:
    type: string
    doc: |
      Type of geography: zip codes or counties
      Valid values: "zip" or "county"
  years:
    type: string[]
    default: ['1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']
    # default: ['2009', '2010']
  bands:
    type: string[]
    doc: gridMET variables or bands
    default: ['rmax', 'rmin', 'sph']
  database:
    type: File
    doc: Path to database connection file, usually database.ini
  connection_name:
    type: string
    doc: The name of the section in the database.ini file
  dates:
    type: string?
    doc: 'dates restriction, for testing purposes only'

steps:
  make_registry:
    run: registry.cwl
    doc: Writes down YAML file with the database model
    in: []
    out:
      - model
      - log
      - errors

  init_tables:
    doc: creates or recreates database tables, one for each band
    scatter:
      - band
    run:
      class: Workflow
      inputs:
        registry:
          type: File
        table:
          type: string
        database:
          type: File
        connection_name:
          type: string
      steps:
        reset:
          run: reset.cwl
          in:
            registry:  registry
            domain:
              valueFrom: "gridmet"
            database: database
            connection_name: connection_name
            table: table
          out:
            - log
            - errors
        index:
          run: index.cwl
          in:
            depends_on: reset/log
            registry: registry
            domain:
              valueFrom: "gridmet"
            table: table
            database: database
            connection_name: connection_name
          out: [log, errors]
      outputs:
        reset_log:
          type: File
          outputSource: reset/log
        reset_err:
          type: File
          outputSource: reset/errors
        index_log:
          type: File
          outputSource: index/log
        index_err:
          type: File
          outputSource: index/errors
    in:
      registry:  make_registry/model
      database: database
      connection_name: connection_name
      band: bands
      geography: geography
      table:
        valueFrom: $(inputs.geography + '_' + inputs.band)
    out:
      - reset_log
      - reset_err
      - index_log
      - index_err

  process:
    doc: Downloads raw data and aggregates it over shapes and time
    scatter:
      - band
      - year
    scatterMethod: nested_crossproduct
    in:
      proxy: proxy
      depends_on: init_tables/index_log
      model: make_registry/model
      shapes: shapes
      geography: geography
      year: years
      dates: dates
      band: bands
      database: database
      connection_name: connection_name
      table:
        valueFrom: $(inputs.geography + '_' + inputs.band)

    run:
      class: Workflow
      inputs:
        depends_on:
          type: Any?
        proxy:
          type: string?
        model:
          type: File
        shapes:
          type: Directory?
        geography:
          type: string
        year:
          type: string
        band:
          type: string
        table:
          type: string
        database:
          type: File
        connection_name:
          type: string
        dates:
          type: string?

      steps:
        download:
          run: download.cwl
          doc: Downloads data
          in:
            year: year
            band: band
            proxy: proxy
          out:
            - data
            - log
            - errors

        get_shapes:
          run: get_shapes.cwl
          in:
            year: year
            geo: geography
            proxy: proxy
          out: [shape_files]

        aggregate:
          run: process.cwl
          doc: Processes data
          in:
            proxy: proxy
            shapes: shapes
            geography: geography
            year: year
            dates: dates
            band: band
            input: download/data
            shape_files: get_shapes/shape_files
          out:
            - data
            - log
            - errors

        ingest:
          run: add_data.cwl
          doc: Uploads data into the database
          in:
            registry: model
            table: table
            input: aggregate/data
            database: database
            connection_name: connection_name
          out: [log, errors]

        vacuum:
          run: vacuum.cwl
          in:
            depends_on: ingest/log
            domain:
              valueFrom: "gridmet"
            registry: model
            table: table
            database: database
            connection_name: connection_name
          out: [log, errors]

      outputs:
        download_log:
          type: File
          outputSource: download/log
        download_err:
          type: File
          outputSource: download/errors

        aggregate_data:
          type: File
          outputSource: aggregate/data
        aggregate_log:
          type: File
          outputSource: aggregate/log
        aggregate_err:
          type: File
          outputSource: aggregate/errors

        ingest_log:
          type: File
          outputSource: ingest/log
        ingest_err:
          type: File
          outputSource: ingest/errors

        vacuum_log:
          type: File
          outputSource: vacuum/log
        vacuum_err:
          type: File
          outputSource: vacuum/errors
    out:
      - download_log
      - download_err
      - aggregate_err
      - aggregate_data
      - aggregate_log
      - ingest_log
      - ingest_err
      - vacuum_log
      - vacuum_err

outputs:
  registry:
    type: File?
    outputSource: make_registry/model
  registry_log:
    type: File?
    outputSource: make_registry/log
  registry_err:
    type: File?
    outputSource: make_registry/errors

  data:
    type:
      type: array
      items:
        type: array
        items: [File]
    outputSource: process/aggregate_data
  download_log:
    type:
      type: array
      items:
        type: array
        items: [File]
    outputSource: process/download_log
  download_err:
    type:
      type: array
      items:
        type: array
        items: [File]
    outputSource: process/download_err

  process_log:
    type:
      type: array
      items:
        type: array
        items: [File]
    outputSource: process/aggregate_log
  process_err:
    type:
      type: array
      items:
        type: array
        items: [File]
    outputSource: process/aggregate_err

  ingest_log:
    type:
      type: array
      items:
        type: array
        items: [File]
    outputSource: process/ingest_log
  ingest_err:
    type:
      type: array
      items:
        type: array
        items: [File]
    outputSource: process/ingest_err

  reset_log:
    type:
      type: array
      items: [File]
    outputSource: init_tables/reset_log
  reset_err:
    type:
      type: array
      items: [File]
    outputSource: init_tables/reset_err

  index_log:
    type:
      type: array
      items: [File]
    outputSource: init_tables/index_log
  index_err:
    type:
      type: array
      items: [File]
    outputSource: init_tables/index_err

  vacuum_log:
    type:
      type: array
      items:
        type: array
        items: [File]
    outputSource: process/vacuum_log
  vacuum_err:
    type:
      type: array
      items:
        type: array
        items: [File]
    outputSource: process/vacuum_err
