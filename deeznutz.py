from concurrent.futures import ThreadPoolExecutor
from pydeezer import Deezer
from tqdm import tqdm
import argparse
import json
import os

def parse_args():
    parser = argparse.ArgumentParser(description='parse args')
    parser.add_argument("--arl", "-a", help="ARL")
    parser.add_argument("--file", "-f", type=argparse.FileType('r'), help="file")
    parser.add_argument("--history", default="history.json", help="file")
    return parser.parse_args()

def load_history(history):
    try:
        with open(history, 'r') as f:
            h = json.load(f)
    except:
        print('Malformed or non-existant history file, creating.')
        with open(history, 'w') as f:
            h = {"finished": {}}
            json.dump(h, f)
    return h

def load_download_list(file, history):
    artist_to_do = [l for l in file]
    print(f"[#] Loaded {len(artist_to_do)} artists from file")
    results = []
    for artist in tqdm(artist_to_do, desc="[>] Searching artists"):
        try:
            results.append(dz.search_artists(artist.strip(), limit=1)['data'][0])
        except:
            print(f"[x] Artist {artist.strip()} not found.")
    results = [artist for artist in results if artist['id'] not in history["finished"].keys()]
    if skipped := len(artist_to_do) - len(results):
        print(f"[#] Skipping {skipped} artists.")
    print(f"[#] Queueing {len(results)} artists for download.")
    return results

def download_gen(download_list):
    for artist in download_list:
        yield artist['id']

def download_track(id):
    print(id)

def fast_download(download_list):
    with ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(download_track, download_gen(download_list))))
    return results

if __name__ == "__main__":
    args = parse_args()
    arl = args.arl or os.environ['DEEZER_ARL']
    dz = Deezer()
    user_info = dz.login_via_arl(arl)
    print(f"[>] Logged in as user {user_info['name']}")
    history = load_history(args.history)
    print(f"[#] Found {len(history['finished'])} completed downloads")
    artist_to_download = load_download_list(args.file, history)
    fast_download(artist_to_download)
