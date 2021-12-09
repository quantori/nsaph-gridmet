#!/usr/bin/env cwl-runner
### Index Builder

cwlVersion: v1.2
class: CommandLineTool
baseCommand: [python, -m, nsaph.loader.index_builder]
requirements:
  InlineJavascriptRequirement: {}

doc: |
  This tool builds all indices for the specified table.
  Log file displays real-time progress of building indices


inputs:
  #$import: db.yaml
  registry:
    type: File?
    inputBinding:
      prefix: --registry
    doc: |
      A path to the data model file
  domain:
    type: string
    inputBinding:
      prefix: --domain
  table:
    type: string
    doc: the name of the table
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
  depends_on:
    type: Any?
    doc: a special field used to enforce dependencies and execution order


outputs:
  log:
    type: File
    outputBinding:
      glob: "*.log"
  errors:
    type: stderr

stderr:  $("index-" + inputs.table + ".err")

