import rasterio
from rasterio.mask import mask
import json
import geopandas as gpd


def clip_to_polygon(in_path: str, bounding_gdf: gpd.GeoDataFrame, buffer=None, out_path=None):
    """clips a raster, saves the clipped raster, opens, and returns
       a handle to it
    
    Arguments:
        in_path {str} -- Path of original raster
        bounding_gdf {gpd.GeoDataFrame} -- gdf where the first line is the bounding geometry
    """
    if buffer:
        bounding_gdf = bounding_gdf.buffer(buffer, join_style=2)

    def get_features(gdf):
    # Function to parse features from GeoDataFrame in such a manner that rasterio wants them
        return [json.loads(gdf.to_json())['features'][0]['geometry']]

    geometry = get_features(bounding_gdf)

    with rasterio.open(in_path) as src:
        out_image, out_transform = rasterio.mask.mask(src, geometry, crop=True)
        out_meta = src.meta

    out_meta.update(
        {
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
        }
    )
    if out_path is None:
        out_path = in_path.split(".tif")[0] + "_clipped.tif"
    
    with rasterio.open(out_path, "w", **out_meta) as dest:
        dest.write(out_image)

    return rasterio.open(out_path)
