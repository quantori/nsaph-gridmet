#!/usr/bin/env cwl-runner
### Model YAML Writer

cwlVersion: v1.2
class: CommandLineTool
baseCommand: [python, -m, gridmet.registry]

doc: |
  This tool writes the data model for gridMET data.


inputs:
  output:
    type: string
    default: "gridmet.yaml"
    doc: A path to a file name with resulting data model
    inputBinding:
      position: 1

outputs:
  log:
    type: File?
    outputBinding:
      glob: "registry*.log"
  model:
    type: File?
    outputBinding:
      glob: "*.yaml"
  errors:
    type: stderr

stderr: registry.err
