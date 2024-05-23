import datetime
import json
import re
import numpy as np
import geopandas as gpd
from datetime import datetime as dt
import pystac
import stackstac
import rioxarray
from minio import Minio
import shutil
import os


def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta

with open('items_group_by_orbit.json', 'r') as f:
    data = json.load(f)

# orbit = 'relative_orbit_63'
# locations = data[orbit][2]

for orbit in data:
    for locations in data[orbit]:
        # generate date for interpolation
        data_by_orbit = data[orbit][locations]
    
        date_start = re.split('[A-Z]', data_by_orbit[-1]['properties']['datetime'])[0]
        date_end = re.split('[A-Z]', data_by_orbit[0]['properties']['datetime'])[0]
        print(f'ORBIT {orbit} - LOCATION {locations}: {date_start}->{date_end}')
        date_range = [date.strftime('%Y-%m-%d') for date in perdelta(datetime.datetime.strptime(date_start, "%Y-%m-%d").date(), 
                                        datetime.datetime.strptime(date_end, "%Y-%m-%d").date(), 
                                        datetime.timedelta(days=12))]
        curr_dates = [re.split('[A-Z]', date['properties']['datetime'])[0] for date in data_by_orbit]
        new_dates = list(set(date_range) - set(curr_dates))
        if new_dates == []:
            continue
        # new_dates = [f"{date}T{re.split('[A-Z]', data_by_orbit[0]['properties']['datetime'])[1]}" for date in new_dates]
        new_dates.sort(key=lambda date: datetime.datetime.strptime(date, "%Y-%m-%d"), reverse=True)
    
        # get just 2 images that the date of new image is between.
        for idx, new_date in enumerate(new_dates):
            for i in range(len(data_by_orbit) - 1):
                if dt.strptime(new_date, "%Y-%m-%d") < dt.strptime(re.split('[A-Z]', data_by_orbit[i]['properties']['datetime'])[0], "%Y-%m-%d") and \
                        dt.strptime(new_date, "%Y-%m-%d") > dt.strptime(re.split('[A-Z]', data_by_orbit[i+1]['properties']['datetime'])[0], "%Y-%m-%d"):
                    items_for_interp = [data_by_orbit[i+1], data_by_orbit[i]]
    
            print(f"DATETIME OF ITEMS: {items_for_interp[0]['properties']['datetime']}->{items_for_interp[1]['properties']['datetime']}")
            try:
                items = pystac.item_collection.ItemCollection(items_for_interp)
                data_arr = stackstac.stack(
                        items,
                        # bounds_latlon=bbox,
                        dtype='float32',
                        assets=["vh"],  # vh only
                        chunksize=2048,
                        resolution=10,
                        epsg=32644
                    ).where(lambda x: x > -32768, other=np.nan)  # sentinel-1-rtc uses -32768 as nodata
                    # .assign_coords(band=lambda x: x.common_name.rename("band"))  # use common names
                
    
                print(f"{new_date}T{re.split('[A-Z]', data_by_orbit[0]['properties']['datetime'])[1]}")
                # interpolate image and write to geotiff
                time = re.split('[A-Z]', data_by_orbit[0]['properties']['datetime'])[1]
                data_interp = data_arr.interp(time=[f"{new_date}T{time}"])
                da_compute = data_interp.isel(time=0).compute()
    
                temp_dir = f'{os.getcwd()}/temp'
                if not os.path.exists(temp_dir):
                    os.makedirs(temp_dir, mode = 0o777)
                file_name = f'{orbit}_{locations}_{idx}_{"".join(re.split("[-|:]", f"{new_date}T{time}"))}.tif'
                file_path = f'{temp_dir}/{file_name}'
                da_compute.rio.to_raster(file_path, tiled=True, windowed=True, driver='COG', BIGTIFF='YES', cache=False)
    
    
                # write file to Minio
                client = Minio("data.skymapglobal.vn",
                    access_key="geoai",
                    secret_key="admin_123",
                )
                bucket_name = "sentinel-s1-rtc-cog-interpolate"
                client.fput_object(
                    bucket_name, file_name, file_path,
                )
    
                print(f'Successful upload {file_name}!')
                print()
                shutil.rmtree(temp_dir)
            except Exception as e:
                print(f'Upload failed!')
                raise Exception(e)
                print()