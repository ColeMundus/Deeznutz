from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from pydeezer.ProgressHandler import BaseProgressHandler
from pydeezer import Deezer
from pydeezer.constants import track_formats
from tqdm import tqdm
import argparse
import json
import os

class MyProgressHandler(BaseProgressHandler):
    def __init__(self):
        pass

    def initialize(self, *args, **kwargs):
        super().initialize(*args, **kwargs)

        pass

    def update(self, *args, **kwargs):
        pass

    def close(self, *args, **kwargs):
        pass

def parse_args():
    parser = argparse.ArgumentParser(description='parse args')
    parser.add_argument("--arl", "-a", required=True, help="arl key")
    parser.add_argument("--file", "-f", required=True, type=argparse.FileType('r'), help="Input file containg a list of artists names separated by newlines")
    parser.add_argument("--history", default="history.json", help="File where download history will be stored")
    parser.add_argument("--workers", "-w", type=int, default=10, help="Number of workers to use for concurrent downloads")
    parser.add_argument("--output", "-o", default="download/", help="Output download directory")
    return parser.parse_args()

def load_history(history_file):
    try:
        with open(history_file, 'r') as f:
            h = json.load(f)
    except:
        print('[\033[1;31;40m!\033[0m] Malformed or non-existant history file, creating.')
        h = {"finished": {}}
        save_history(history_file, h)
    return h

def save_history(history_file=False, h=False):
    if not history_file:
        history_file = args.history
    if not h:
        h = history
    with open(history_file, 'w') as f:
        json.dump(h, f, indent=4, sort_keys=True)

def load_download_list(file, history):
    artist_to_do = [l for l in file]
    print(f"[\033[1;33;40m#\033[0m] Loaded {len(artist_to_do)} artists from file")
    results = []
    for artist in tqdm(artist_to_do, desc="[\033[1;34;40m>\033[0m] Searching artists"):
        try:
            results.append(dz.search_artists(artist.strip(), limit=1)['data'][0])
        except:
            print(f"[\033[1;31;40x\033[0m] Artist {artist.strip()} not found.")
    if skipped := len(artist_to_do) - len(results):
        print(f"[\033[1;33;40mx\033[0m] Skipping {skipped} artists.")
    print(f"[\033[1;33;40m#\033[0m] Queueing {len(results)} artists for download.")
    return results

def download_queue(download_list):
    for artist in download_list:
        for album_info in dz.get_artist_discography(artist['id'])['data']:
            album_id = str(album_info['ALB_ID'])
            if album_id in history["finished"] and history["finished"][album_id]["finished"] == True:
                print(f"[\033[1;33;40mx\033[0m] Already downloaded album {album_info['ALB_TITLE']}")
            else:
                album, _ = dz.get_album(int(album_id))
                for n, track in enumerate(album['tracks']['data']):
                    if album_id in history["finished"] and track['id'] in history["finished"][album_id]["finished_tracks"]:
                        print(f"[\033[1;33;40mx\033[0m] Already downloaded track {track['title']}")
                    else:
                        yield (n, track, album)

def download_track(meta):
    n, track, album = meta
    dl_dir = f"{args.output.strip('/')}/{album['artist']['name']}/{album['artist']['name'] - album['title']}/"
    filename = f"{n+1} - {track['title']}.mp3"
    filename = "".join([c for c in filename if c.isalpha() or c.isdigit() or c in ' []()-,.!?:;']).rstrip()
    out = album['artist']['name'], album['title'], str(album['id']), track['id'], filename, len(album['tracks']['data'])
    try:
        tk = dz.get_track(track['id'])['info']
        dz.download_track(tk, dl_dir, filename=filename, quality=track_formats.MP3_320, show_messages=False, progress_handler=MyProgressHandler())
    except:
        return True, *out
    return False, *out

def concurrent_download(download_list):
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(download_track, meta) for meta in download_queue(download_list)]
        for future in concurrent.futures.as_completed(futures, timeout=240):
            error, artist, album, album_id, track_id, filename, count = future.result()
            if album_id not in history["finished"]:
                history["finished"][album_id] = {"name": album, "finished_tracks": [], "error_tracks": [], "finished": False}
            if error:
                print(f"[\033[1;31;40m!\033[0m] Error downloading: \033[1;31;40m{artist}/{album}/{filename}\033[0m")
                history["finished"][album_id]["error_tracks"].append(track_id)
            else:
                history["finished"][album_id]["finished_tracks"].append(track_id)
                total_count = len(history["finished"][album_id]["finished_tracks"]) + len(history["finished"][album_id]["error_tracks"])
                if count == total_count:
                    history["finished"][album_id]["finished"] = True
                save_history()
                print(f"[\033[1;32;40m<\033[0m] \033[1;32;40mFinished downloading: \033[1;33;40m{artist}/\033[1;34;40m{album}/\033[1;35;40m{filename}\033[0m")

if __name__ == "__main__":
    from gevent import monkey
    monkey.patch_all(thread=False, socket=False)
    args = parse_args()
    dz = Deezer()
    user_info = dz.login_via_arl(args.arl)
    print(f"[\033[1;32;40m<\033[0m] Logged in as user {user_info['name']}")
    history = load_history(args.history)
    print(f"[\033[1;33;40m#\033[0m] Found {len([h for h in history['finished'] if history['finished'][h]['finished'] == True])} completed downloads")
    artist_to_download = load_download_list(args.file, history)
    concurrent_download(artist_to_download)
