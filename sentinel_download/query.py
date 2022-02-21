import os
import shutil
import glob
from pathlib import Path

import json
import requests

from sentinelsat import SentinelAPI
from download import download_list


def download_quicklooks(
    tile,
    wrk_dir,
    start_date,
    end_date,
    cop_user,
    cop_pass,
    platform_name,
    product_type
):

    """
    Downloads a list of quicklooks for visual investigation/confirmation between the two specified dates
    and for the sensor and product type given.

        tile : Name of the Sentinel tile (e.g. 32VNH)
        wrk_dir : Your working directory
        start_date and end_date : Starting and ending date of the images downloaded in format YYYYMMDD
        cop_user and cop_pass : Username and password for the Copernicus portal https://www.copernicus.eu/en
        platform_name : 'Sentinel-1' or 'Sentinel-2'
        product_type : The processing level or product type of the image (e.g. S2MSI1C)
    """

    api_Sentinel = SentinelAPI(cop_user, cop_pass)

    results = api_Sentinel.query(
        platformname = platform_name,
        producttype = product_type,
        cloudcoverpercentage = (0,30),
        tileid = tile,
        date = (start_date, end_date)
    )

    Path(os.path.join(wrk_dir, tile)).mkdir(exist_ok=True)
    tiledir = os.path.join(wrk_dir, tile, 'quicklooks')
    Path(tiledir).mkdir(exist_ok=True)

    for pid, data in results.items():
        url = data["link_icon"]
        r = api_Sentinel.session.get(url)
        path = os.path.join(tiledir, '{}.jpeg'.format(data['identifier']))
        with open(path, 'wb') as f:
            f.write(r.content)

def get_uids(scene_dates, wrk_dir):
    """
    Retrieves the formal uid required for api download from Creodias.

    scene_dates : Dictionary of tile:date combinations for downloading
        (e.g. '32VNH' : ['20200414', '20210420'])
    """
    scene_dirs = []
    for tile in scene_dates.keys():
        tiledir = os.path.join(wrk_dir, tile, 'quicklooks')
        for date in scene_dates[tile]:
            try:
                scene_dir = glob.glob(os.path.join(
                    wrk_dir, tile, 'quicklooks', f'*{date}*{tile}*.jpeg'))[0]
                scene_dirs.append(scene_dir)
            except IndexError:
                print(f'{tile}_{date} could not be located.')

    scene_ids = [scene.split('\\')[-1][:-5] for scene in scene_dirs]

    finder_api_urls = []
    for scene in scene_ids:
        finder_api_urls.append(
            'https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json?&productIdentifier=%25'
            + scene + '%25&dataset=ESA-DATASET'
        )

        uids = []
        for finder_api_url in finder_api_urls:
            response = requests.get(finder_api_url)
            for feature in json.loads(response.content.decode('utf-8'))['features']:
                uids.append(feature['id'])

    return uids

def move_scenes(scene_dates, wrk_dir):
    tiles = list(scene_dates.keys())
    for tile in tiles:
        src_dirs = glob.glob(os.path.join(wrk_dir, 'scenes', f'*{tile}*.SAFE'))
        for src_dir in src_dirs:
            sensor, date = src_dir.split('\\')[-1].split('_')[0], src_dir.split('\\')[-1].split('_')[2][0:8]
            dst_dir = os.path.join(wrk_dir, tile, f'{sensor}_{date}.SAFE')
            shutil.move(src_dir, dst_dir)

def get_sentinel_images(wrk_dir, scene_dates, creo_user, creo_pass, threads=5):

    uids = get_uids(scene_dates, wrk_dir)
    download_list(uids, creo_user, creo_pass, wrk_dir, threads=threads)
    move_scenes(scene_dates, wrk_dir)
