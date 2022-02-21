import os
import shutil
import zipfile
from pathlib import Path

import json
import requests
import concurrent.futures

from multiprocessing.pool import ThreadPool
from tqdm import tqdm

"""
Downloads Sentinel images concurrently from Creodias based on a query search.

Username and password refer to your credentials for https://creodias.eu/
"""

def get_keycloak_token(username, password):
    # Admin console and create a new client.
    h = {
        'Content-Type': 'application/x-www-form-urlencoded'
        }
    d = {
        'client_id': 'CLOUDFERRO_PUBLIC',
        'password': password,
        'username': username,
        'grant_type': 'password'
        }
    resp = requests.post('https://auth.creodias.eu/auth/realms/dias/protocol/openid-connect/token', data=d, headers=h)

    try:
        token = json.loads(resp.content.decode('utf-8'))['access_token']

    except KeyError:
        print("Can't obtain a token (check username/password), exiting.")
        sys.exit()

    return token

def _download_raw_data(url, outfile, show_progress):
    # Downloads data from url to outfile.incomplete and then moves to outfile
    outfile_temp = str(outfile) + ".incomplete"

    try:
        downloaded_bytes = 0
        with requests.get(url, stream=True, timeout=100) as req:
            with tqdm(unit="B", unit_scale=True, disable=not show_progress) as progress:
                chunk_size = 2 ** 20  # download in 1 MB chunks
                with open(outfile_temp, "wb") as fout:
                    for chunk in req.iter_content(chunk_size=chunk_size):
                        if chunk:  # filter out keep-alive new chunks
                            fout.write(chunk)
                            progress.update(len(chunk))
                            downloaded_bytes += len(chunk)
        shutil.move(outfile_temp, outfile)

    finally:
        try:
            Path(outfile_temp).unlink()
        except OSError:
            pass

def download_list(uids, username, password, wrk_dir, threads=1, show_progress=True):
    # Download a list of scenes concurrently
    if threads > len(uids):
        threads = len(uids)

    if show_progress:
        pbar = tqdm(total=len(uids), unit="files")

    def _download(uid):
        Path(os.path.join(wrk_dir, 'scenes')).mkdir(exist_ok=True)
        outdir = os.path.join(wrk_dir, 'scenes')
        outfile = Path(outdir) / f"{uid}.zip"
        token = get_keycloak_token(username, password)
        url = f'https://zipper.creodias.eu/download/{uid}?token={token}'
        _download_raw_data(url, outfile, show_progress=False)
        if show_progress:
            pbar.update(1)
        return uid, outfile

    with concurrent.futures.ThreadPoolExecutor(threads) as executor:
        paths = dict(executor.map(_download, uids))

    for zip_path in paths.values():
        with zipfile.ZipFile(zip_path, 'r') as zip_:
            zip_.extractall(os.path.join(wrk_dir, 'scenes'))
        os.remove(zip_path)

    return paths
