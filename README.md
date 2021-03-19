NSAPH gridMET Read Me First
==================================

gridMET is a dataset of daily high-spatial resolution (~4-km, 1/24th degree) 
surface meteorological data covering the contiguous US from 1979-yesterday. 
The data are also known and cited as METDATA.

Executing pipelines from this package require a collection of shape files
corresponding to geographies for which data is aggregated
(for example, zip code areas or counties).

The data has to be placed in the following directory structure:
`${year}/${geo_type: zip|county|etc.}/${shape:point|polygon}/`

Which geography is used is defined by `geography` argument that defaults
to "zip". Only actually used geographies must have their shape files
for the years actually used.

Detailed Documentation
----------------------

Hosted [Documentation](https://nsaph-sandbox01.rc.fas.harvard.edu/gridmet/index.html) 
describes Python code and API

Usage
-----

    usage: gridmet.py [-h] --variable
                      {bi,erc,etr,fm100,fm1000,pet,pr,rmax,rmin,sph,srad,th,tmmn,tmmx,vpd,vs}
                      [{bi,erc,etr,fm100,fm1000,pet,pr,rmax,rmin,sph,srad,th,tmmn,tmmx,vpd,vs} ...]
                      [--strategy {default,all_touched,combined}]
                      [--destination DESTINATION] [--downloads DOWNLOADS]
                      [--geography GEOGRAPHY] [--shapes_dir SHAPES_DIR]
                      [--shapes [SHAPES [SHAPES ...]]]
    
    optional arguments:
      -h, --help            show this help message and exit
      --years [YEARS [YEARS ...]], -y [YEARS [YEARS ...]]
                            Year or list of years to download. For example, the
                            following argument: `-y 1992:1995 1998 1999 2011
                            2015:2017` will produce the following list:
                            [1992,1993,1994,1995,1998,1999,2011,2015,2016,2017] ,
                            default: 1990:2020
      --compress, -c        Use gzip compression for the result, default: True
      --variables {bi,erc,etr,fm100,fm1000,pet,pr,rmax,rmin,sph,srad,th,tmmn,tmmx,vpd,vs} [{bi,erc,etr,fm100,fm1000,pet,pr,rmax,rmin,sph,srad,th,tmmn,tmmx,vpd,vs} ...], --var {bi,erc,etr,fm100,fm1000,pet,pr,rmax,rmin,sph,srad,th,tmmn,tmmx,vpd,vs} [{bi,erc,etr,fm100,fm1000,pet,pr,rmax,rmin,sph,srad,th,tmmn,tmmx,vpd,vs} ...]
                            Gridmet bands or variables
      --strategy {default,all_touched,combined}, -s {default,all_touched,combined}
                            Rasterization Strategy, default: default
      --destination DESTINATION, --dest DESTINATION, -d DESTINATION
                            Destination directory for the processed files,
                            default: data/processed
      --raw_downloads RAW_DOWNLOADS
                            Directory for downloaded raw files, default:
                            data/downloads
      --geography GEOGRAPHY
                            The type of geographic area over which we aggregate
                            data, default: zip
      --shapes_dir SHAPES_DIR
                            Directory containing shape files for geographies.
                            Directory structure is expected to be:
                            .../${year}/${geo_type}/{point|polygon}/, default:
                            shapes
      --shapes [SHAPES [SHAPES ...]]
                            Type of shapes to aggregate over, default: ['polygon']
    
Example
-------

One can try it on `nsaph-sandbox01.rc.fas.harvard.edu` changing to folder:
`/data/projects/gridmet/`

and running the following command (do not forget `-u` option, or you 
will not be able to see the progress):

`source /home/nsaph/projects/tools/gridmet/.gridmet/bin/activate && PYTHONPATH=/home/nsaph/projects/tools/gridmet/src/python python -u -m gridmet --var tmmx -y 2001 --shapes_dir shapes/zip_shape_files --strategy downscale`

The results can be then found in data/processed folder
