#!/usr/bin/env cwl-runner
### File rearranger

cwlVersion: v1.2
class: CommandLineTool
baseCommand: mv

doc: |
  This tool rearranges downloaded files for further ingestion into a database.

requirements:
  InlineJavascriptRequirement: {}
  InitialWorkDirRequirement:
    listing:
      - $(inputs.band)
  

inputs:
  source:
  band:
    type: string
    inputBinding:
      position: 2

outputs:
  data:
    type: Directory
    outputBinding:
      glob: "*.yaml"
  errors:
    type: stderr

  $("rearranger-" + inputs.band + ".err")