# TODO: Test this out
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
from pyproj import CRS


def create_grid(dx, bounding_gdf):
    """[summary]
    
    Arguments:
        dx {int} -- distance between gridpoints in meters
        bounding_gdf {gpd.GeoDataFrame} -- GeoDataframe conaining bounding geometry.
    """
    xmin, ymin, xmax, ymax = bounding_gdf.total_bounds

    # create iterators for 10*10 km grid
    xrange = range(int(xmin), int(xmax), dx)
    yrange = range(int(ymin), int(ymax), dx)
    points = []

    for x in xrange:
        for y in yrange:
            points.append(Point(x, y))

    grid = gpd.GeoDataFrame({"geometry": points})
    grid.crs = CRS.from_epsg(32632)
    grid = grid[grid.geometry.within(bounding_gdf.geometry[0])]
    return grid


def density_grid(gdf, buffer, dx=None, bounding_gdf=None):

    if (dx is None) or (bounding_gdf is None):
        raise ValueError("dx and bounding_gdf have to be supplied.")
    grid = create_grid(dx=dx, bounding_gdf=bounding_gdf)

    if buffer < dx:
        raise ValueError("Buffer cannot be less than dx")
    grid["counts"] = grid.buffer(buffer).envelope.apply(
        lambda g: gdf.geometry.centroid.intersects(g).sum()
    )
    buffered_area = (2 * buffer) ** 2
    grid["density"] = grid.counts / (buffered_area * 10 ** (-4))
    return grid
