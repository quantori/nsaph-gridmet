#!/usr/bin/env cwl-runner
### Uploader of the gridMET Data to the database

cwlVersion: v1.2
class: CommandLineTool
baseCommand: [python, -m, nsaph.loader.data_loader]
# baseCommand: echo
requirements:
  InlineJavascriptRequirement: {}

doc: |
  This tool uploads the data to the database


inputs:
  registry:
    type: File
    inputBinding:
      prefix: --registry
    doc: |
      A path to the data model file
  table:
    type: string
    doc: the name of the table to be created
    inputBinding:
      prefix: --table
  database:
    type: File
    doc: Path to database connection file, usually database.ini
    inputBinding:
      prefix: --db
  connection_name:
    type: string
    doc: The name of the section in the database.ini file
    inputBinding:
      prefix: --connection
  input:
    type: File[]
    inputBinding:
      prefix: --data
    doc: |
      A path the downloaded data files
  pattern:
    type: string
    default: "*.csv*"
    inputBinding:
      prefix: --pattern
  threads:
    type: int
    default: 4
    doc: number of threads, concurrently writing into the database
  page_size:
    type: int
    default: 1000
    doc: explicit page size for the database
  log_frequency:
    type: long
    default: 100000
    doc: informational logging occurs every specified number of records
  limit:
    type: long?
    doc: |
      if specified, the process will stop after ingesting
      the specified number of records
  depends_on:
    type: Any?
    doc: a special field used to enforce dependencies and execution order

arguments:
    - valueFrom: "--reset"
    - valueFrom: "gridmet"
      prefix: --domain


outputs:
  log:
    type: File?
    outputBinding:
      glob: "*.log"
  errors:
    type: stderr

stderr:  $("ingest-" + inputs.table + ".err")

