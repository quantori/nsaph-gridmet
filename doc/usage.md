Using Gridmet Package
=====================

Executing pipelines through this class requires a collection of shape files
corresponding to geographies for which data is aggregated
(for example, zip code areas or counties).

The data has to be placed in teh following directory structure:
`${year}/${geo_type: zip|county|etc.}/${shape:point|polygon}/`

Which geography is used is defined by `geography` argument that defaults
to "zip". Only actually used geographies must have their shape files
for the years actually used.


    usage: gridmet.py [-h] --variable
                      {bi,erc,etr,fm100,fm1000,pet,pr,rmax,rmin,sph,srad,th,tmmn,tmmx,vpd,vs}
                      [{bi,erc,etr,fm100,fm1000,pet,pr,rmax,rmin,sph,srad,th,tmmn,tmmx,vpd,vs} ...]
                      [--strategy {default,all_touched,combined}]
                      [--destination DESTINATION] [--downloads DOWNLOADS]
                      [--geography GEOGRAPHY] [--shapes_dir SHAPES_DIR]
                      [--shapes [SHAPES [SHAPES ...]]]
    
    optional arguments:
      -h, --help            show this help message and exit
      --variable {bi,erc,etr,fm100,fm1000,pet,pr,rmax,rmin,sph,srad,th,tmmn,tmmx,vpd,vs} [{bi,erc,etr,fm100,fm1000,pet,pr,rmax,rmin,sph,srad,th,tmmn,tmmx,vpd,vs} ...], --var {bi,erc,etr,fm100,fm1000,pet,pr,rmax,rmin,sph,srad,th,tmmn,tmmx,vpd,vs} [{bi,erc,etr,fm100,fm1000,pet,pr,rmax,rmin,sph,srad,th,tmmn,tmmx,vpd,vs} ...]
                            Gridmet variable
      --strategy {default,all_touched,combined}, -s {default,all_touched,combined}
                            Rasterization Strategy, default:
                            RasterizationStrategy.DEFAULT
      --destination DESTINATION, --dest DESTINATION, -d DESTINATION
                            Destination directory for the processed files,
                            default: data/processed
      --downloads DOWNLOADS
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
                            Type of shapes to aggregate over, default: polygon
