# TODO: Test this out
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
from pyproj import CRS
import matplotlib.colors as colors
import rasterio
from rasterio.transform import from_origin

# To make this shit into a raster:
# 1. Save length and height of thedataset as ints,
# 2. Possibly keep the full square as a numpy array.
# 3. Once you have an array, store it as a raster with
#    the correct metadata.

class DensityGrid:

    def __init__(self, dx=None, bounding_gdf=None, buffer=None, path=None):
        if (dx and (bounding_gdf is not None) and buffer) and (path is None):
            if buffer < dx:
                raise ValueError("buffer must be larger than or equal to dx, otherwise points are missed")
            self.dx = dx
            self.bounding_gdf = bounding_gdf
            self.buffer = buffer
            self._grid = None
            self.create_grid()
            self.data_columns = []
        elif path:
            self._grid = gpd.GeoDataFrame.from_file(path)
        else:
            raise ValueError("Either path must be supplied, or dx, bounding_gdf, and buffer must all be supplied")

    @property
    def grid(self):
        return self._grid
    
    def get_array(self, column):
        return np.flip(
            self._grid[column].values.reshape((self.n_rows, self.n_cols)),
            axis=0
        ).astype(np.float)
    
    def save_raster(self, column, path):
        array = self.get_array(column)
        transform = from_origin(self.xmin, self.ymax, self.dx, self.dx)
        new_raster = rasterio.open(
            path, mode="w", driver="GTiff",
            height=array.shape[0],
            width=array.shape[1],
            count=1,
            dtype=str(array.dtype),
            crs="EPSG:32632",
            transform=transform
        )
        new_raster.write(array, 1)
        new_raster.close()
    
    def save_rasters(self, path_prefix="./"):
        for col_name in self.data_columns:
            path = path_prefix + col_name + ".tif"
            self.save_raster(col_name, path)

    def save_grid(self, path):
        """Saves grid as GEOJSON
        """        
        self._grid.to_file(path, driver="GeoJSON")

    def plot(self, column, ax, vmax=None, norm=None, legend=False):
        if norm == 'log':
            normalize_method = colors.LogNorm(vmin=1e-5, vmax=vmax)
        else:
            normalize_method = colors.Normalize(vmin=1e-5, vmax=vmax)

        ax = self._grid.plot(
            column=column,
            ax=ax,
            norm=normalize_method,
            legend=legend
        )
        ax.set_title(' '.join(column.split('_')))
        ax.set_xlabel("meters utm 32N")
        ax.set_xticklabels(ax.get_xticklabels(), rotation=25)
        return ax
    
    def create_grid(self):
        """Creates a grid of points dx apart within a bounding geometry.
        """
        self.xmin, self.ymin, self.xmax, self.ymax = self.bounding_gdf.total_bounds

        # create iterators for dx*dx m grid
        xrange = range(int(self.xmin), int(self.xmax), self.dx)
        yrange = range(int(self.ymin), int(self.ymax), self.dx)
        
        # Stored to be able to create an array afterwards
        self.n_cols = len(xrange)
        self.n_rows = len(yrange)
        points = []

        for y in yrange:
            for x in xrange:
                points.append(Point(x, y))

        grid = gpd.GeoDataFrame({"geometry": points})
        grid.crs = CRS.from_epsg(32632)
        # grid = grid[grid.geometry.within(self.bounding_gdf.geometry.iloc[0])]  # uncomment to store only the array
        grid["within"] = grid.geometry.within(self.bounding_gdf.geometry.iloc[0])
        grid["geometry"] = grid.buffer(self.dx/2).envelope
        self._grid = grid
        

    def create_density(self, gdf, append=False, suffix=None):
        """
        Returns grid with station counts within a bounding square of [(buffer*2)^2] m^2 around each point.
        The resulting GeoDataFrame also contains a column "density" of stations per km^2.
        The buffer argument can be thought of as a smoothing parameter.
        """ 
        cols = {'counts': 'counts', 'density': 'density'}
        if append and (suffix is not None):
            cols = {key: val + "_" + suffix for key, val in cols.items()} 

        def g(row):
            if row.within:
                return gdf.geometry.centroid.intersects(
                    row.geometry.buffer(self.buffer-self.dx).envelope
                    ).sum()
            else:
                return np.nan

        self._grid[cols["counts"]] = self._grid.apply(g, axis=1)
        buffered_area = (2 * self.buffer) ** 2
        self._grid[cols["density"]] = self._grid[cols["counts"]] / (buffered_area * (10 ** -3) ** 2)
        self.data_columns += [cols["counts"], cols["density"]]