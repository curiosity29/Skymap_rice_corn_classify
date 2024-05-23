from osgeo import gdal
import geopandas as gpd
import pandas as pd
import pystac_client
from tqdm import tqdm
import os
import warnings
warnings.filterwarnings('ignore')
tqdm.pandas()
aoi=gpd.read_file("../aoi/AndhraPradesh.geojson")
catalog = pystac_client.Client.open("https://stactest.eofactory.ai")
search = catalog.search(
    # collections=["sentinel-1-rtc" , "sentinel-s1-rtc-cog-interpolate"],
    collections=["sentinel-1-rtc"],
    intersects=aoi.geometry[0],
)
items=search.item_collection()
data = {
    "datetime": [item.datetime for item in items],
    "href": ["/vsicurl/" + (item.assets.get("vh") or item.assets.get("image")).href for item in items]
}
df = pd.DataFrame.from_dict(data).set_index("datetime")
df.sort_index(inplace=True)
os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "YES"
os.environ["AWS_VIRTUAL_HOSTING"] = "NO"
os.environ["AWS_S3_ENDPOINT"] = 'data.skymapglobal.vn'
os.environ["AWS_ACCESS_KEY_ID"] = "geoai"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin_123"
os.environ["CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE"] = "YES"
def mosaic(group):
    out_folder = f"/vsis3/sentinel-1-rtc-mosaic-andhra-pradesh/MOSAICED_{group.name.strftime('%Y%m%dT%H%M%S')}"
    # src = gdal.Open(f"{out_folder}/VH.tif")
    # if src:
    #     break
    gdal.Warp(f"{out_folder}/VH.tif", group.href.tolist(), format="COG", options="-overwrite -multi -wm 80% -t_srs EPSG:32644 -co TILED=YES -co BIGTIFF=YES -co COMPRESS=DEFLATE -co NUM_THREADS=ALL_CPUS")
df.groupby(pd.Grouper(freq='12D', origin='epoch')).progress_apply(mosaic)