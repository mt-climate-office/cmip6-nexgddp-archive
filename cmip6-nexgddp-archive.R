# pak::pak(
#   c("magrittr",
#     "tidyverse",
#     "multidplyr",
#     "sf",
#     "terra",
#     "ncdf4",
#     "mt-climate-office/cmip6")
# )
purrr::walk(
  c("magrittr",
    "tidyverse",
    "multidplyr",
    "sf",
    "terra"),
  library,
  character.only = TRUE)

# Allow terra to use a lot of memory
terra::terraOptions(memfrac = 0.9,
                    memmin = 16)

## Create a boundary for the area of interest, defined as the hydrologic basins
## of the Contiguous US (CONUS)
if(!file.exists("data/huc2.parquet")){
  sf::read_sf(
    dsn = "/vsizip//vsicurl/https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/National/GDB/WBD_National_GDB.zip/WBD_National_GDB.gdb/", 
    layer = "WBDHU2") %>%
    sf::st_make_valid() %>%
    dplyr::transmute(huc2 = as.integer(huc2), 
                     name) %>%
    dplyr::group_by(huc2, name) %>%
    dplyr::summarise(.groups = "drop") %>%
    sf::st_cast("MULTIPOLYGON") %>%
    sf::write_sf(file.path("data", "huc2.parquet"),
                 layer_options = c("COMPRESSION=BROTLI",
                                   "GEOMETRY_ENCODING=GEOARROW",
                                   "WRITE_COVERING_BBOX=NO"),
                 driver = "Parquet")
}


list(
  conus = 
    sf::read_sf("data/huc2.parquet") %>%
    dplyr::filter(huc2 %in% 1:18) %>%
    dplyr::summarise() %>%
    sf::st_transform("EPSG:5070") %T>%
    sf::write_sf(file.path("data", "conus.parquet"),
                 layer_options = c("COMPRESSION=BROTLI",
                                   "GEOMETRY_ENCODING=GEOARROW",
                                   "WRITE_COVERING_BBOX=NO"),
                 driver = "Parquet"),
  
  ak = 
    sf::read_sf("data/huc2.parquet") %>%
    dplyr::filter(huc2 %in% 19) %>%
    dplyr::summarise() %>%
    sf::st_transform("EPSG:3338") %T>%
    sf::write_sf(file.path("data", "ak.parquet"),
                 layer_options = c("COMPRESSION=BROTLI",
                                   "GEOMETRY_ENCODING=GEOARROW",
                                   "WRITE_COVERING_BBOX=NO"),
                 driver = "Parquet"),
  
  hi = 
    sf::read_sf("data/huc2.parquet") %>%
    dplyr::filter(huc2 %in% 20) %>%
    dplyr::summarise() %>%
    sf::st_crop(xmin = -161, xmax = 0, ymin = -90, ymax = 90) %>%
    sf::st_transform("ESRI:102007") %T>%
    sf::write_sf(file.path("data", "hi.parquet"),
                 layer_options = c("COMPRESSION=BROTLI",
                                   "GEOMETRY_ENCODING=GEOARROW",
                                   "WRITE_COVERING_BBOX=NO"),
                 driver = "Parquet"),
  
  pr =
    sf::read_sf("data/huc2.parquet") %>%
    dplyr::filter(huc2 %in% 21) %>%
    dplyr::summarise() %>%
    sf::st_transform("EPSG:3991") %T>%
    sf::write_sf(file.path("data", "pr.parquet"),
                 layer_options = c("COMPRESSION=BROTLI",
                                   "GEOMETRY_ENCODING=GEOARROW",
                                   "WRITE_COVERING_BBOX=NO"),
                 driver = "Parquet")
) |>
  purrr::iwalk(\(x,y) cmip6::cmip6_dl(
    outdir = file.path("/Volumes/SSD8/cmip6", y),
    # aoi = x,
    models = 
      c("ACCESS-ESM1-5",
        "CNRM-ESM2-1",
        "EC-Earth3",
        "GFDL-ESM4",
        "GISS-E2-1-G",
        "MIROC6",
        "MPI-ESM1-2-HR",
        "MRI-ESM2-0")
  ))
