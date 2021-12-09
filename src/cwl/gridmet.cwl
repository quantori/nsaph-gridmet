#!/usr/bin/env cwl-runner
### gridMET Pipeline

cwlVersion: v1.2
class: Workflow

requirements:
  SubworkflowFeatureRequirement: {}
  StepInputExpressionRequirement: {}
  InlineJavascriptRequirement: {}
  ScatterFeatureRequirement: {}
  MultipleInputFeatureRequirement: {}

doc: |
  Downloads, processes gridMET data and ingests it into the database

inputs:
  proxy:
    type: string?
    default: ""
    doc: HTTP/HTTPS Proxy if required
  shapes:
    type: Directory
  geography:
    type: string
    doc: |
      Type of geography: zip codes or counties
  years:
    type: string[]
    #default: ['1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']
  bands:
    type: string[]
    default: ['tmmx', 'rmax']
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
  registry:
    run: registry.cwl
    doc: Writes down YAML file with the database model
    in: []
    out:
      - model
      - log
      - errors

  process:
    scatter: band
    in:
      proxy: proxy
      model: registry/model
      shapes: shapes
      geography: geography
      years: years
      dates: dates
      band: bands
      database: database
      connection_name: connection_name
      table:
        valueFrom: $(inputs.geography + '_' + inputs.band)

    run:
      class: Workflow
      inputs:
        proxy:
          type: string?
        model:
          type: File
        shapes:
          type: Directory
        geography:
          type: string
        years:
          type: string[]
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
          doc: Downloads and processes data
          scatter: year
          scatterMethod:  nested_crossproduct
          in:
            proxy: proxy
            shapes: shapes
            geography: geography
            year: years
            dates: dates
            band: band
          out:
            - data
            - log
            - errors

        ingest:
          run: ingest.cwl
          doc: Uploads data into the database
          in:
            registry: model
            table: table
            input: download/data
            database: database
            connection_name: connection_name
          out: [log, errors]

        index:
          run: index.cwl
          in:
            depends_on: ingest/log
            registry: model
            domain:
              valueFrom: "gridmet"
            table: table
            database: database
            connection_name: connection_name
          out: [log, errors]

        vacuum:
          run: vacuum.cwl
          in:
            depends_on: index/log
            domain:
              valueFrom: "gridmet"
            registry: model
            table: table
            database: database
            connection_name: connection_name
          out: [log, errors]
      outputs:
        data:
          type: File[]
          outputSource: download/data
        download_log:
          type: File[]
          outputSource: download/log
        download_err:
          type: File[]
          outputSource: download/errors

        ingest_log:
          type: File
          outputSource: ingest/log
        ingest_err:
          type: File
          outputSource: ingest/errors

        index_log:
          type: File
          outputSource: index/log
        index_err:
          type: File
          outputSource: index/errors

        vacuum_log:
          type: File
          outputSource: vacuum/log
        vacuum_err:
          type: File
          outputSource: vacuum/errors
    out:
      - data
      - download_log
      - download_err
      - ingest_log
      - ingest_err
      - index_log
      - index_err
      - vacuum_log
      - vacuum_err



outputs:
  registry:
    type: File?
    outputSource: registry/model
  registry_log:
    type: File?
    outputSource: registry/log
  registry_err:
    type: File?
    outputSource: registry/errors

  data:
    type:
      type: array
      items:
        type: array
        items: [File]
    outputSource: process/data
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

  ingest_log:
    type: File[]
    outputSource: process/ingest_log
  ingest_err:
    type: File[]
    outputSource: process/ingest_err

  index_log:
    type: File[]
    outputSource: process/index_log
  index_err:
    type: File[]
    outputSource: process/index_err

  vacuum_log:
    type: File[]
    outputSource: process/vacuum_log
  vacuum_err:
    type: File[]
    outputSource: process/vacuum_err
