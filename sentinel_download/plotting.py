import os

import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as cx

from shapely.geometry import box
from mpl_toolkits.basemap import Basemap
from sentinelsat import SentinelAPI

def locate_tiles(
    aoi,
    sentinel_tiling_grid,
    cop_user,
    cop_pass,
    platform_name,
    product_type,
    relation_type='Intersects',
    plot_graphics=True
):

    """
    Returns the sentinel tile names either intersected or contained by the AOI.

        aoi : a shapefile or GEOJSON of the area
        sentinel_tile_locator : a shapefile or GEOJSON of the data found here:
            https://github.com/justinelliotmeyers/Sentinel-2-Shapefile-Index
        cop_user and cop_pass : Username and password for the Copernicus portal https://www.copernicus.eu/en
        platform_name : 'Sentinel-1' or 'Sentinel-2'
        product_type : The processing level or product type of the image (e.g. S2MSI1C)
        relation_type : The topological relationship between the aoi and tile, 'intersects' or 'contains'
        plot_graphics : True outputs a map for visual validation
    """

    AOI = gpd.read_file(aoi).to_crs(epsg=4326)
    #AOI = AOI.to_crs('EPSG:4326')
    bounds_AOI = AOI.total_bounds
    bbox = box(*bounds_AOI)

    api_Sentinel = SentinelAPI(cop_user, cop_pass)

    # Query scenes
    product = api_Sentinel.query(
        bbox,
        date = ('NOW-14DAYS', 'NOW'),
        platformname = platform_name,
        producttype = product_type,
        area_relation = relation_type,
        limit = 20
    )

    tileList = []
    for value in product.values():
        tile = value['tileid']
        tileList.append(tile)

    grid = gpd.read_file(sentinel_tiling_grid).to_crs(epsg=4326)
    tile_selection = grid[grid['Name'].isin(tileList)]
    tiles = tile_selection.Name.values.tolist()

    if plot_graphics:
        tile_plot = tile_selection.plot(
            figsize=(12,12),
            facecolor="none",
            edgecolor='blue',
            lw=1,
            legend=True,
            legend_kwds={
                'bbox_to_anchor':(0, 1.05),
                'fontsize':10,
            }
        )

        ax = AOI.plot(
            ax=tile_plot,
            facecolor="none",
            lw=2,
            edgecolor='red',
            legend=True,
            legend_kwds={
                'bbox_to_anchor' : (2, 1.05),
                'fontsize' : 15,
            }
        )

        for i, geo in tile_selection.centroid.iteritems():
            ax.annotate(
                text=tile_selection.Name[i],
                xy=[geo.x, geo.y],
                color="blue",
                fontsize = 15
            )

        cx.add_basemap(ax,
            source=cx.providers.CartoDB.Voyager,
            zoom=12
        )

    return (tiles)
