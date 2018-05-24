from multiprocessing import Pool, cpu_count
import os
import numpy as np
import shutil
import json
import pandas as pd
import datetime

BASEPATH='data/'
BASE_MPD_FILENAMES = [ BASEPATH + "mpd/" + f for i, f in enumerate(os.listdir(BASEPATH+"mpd/")) if f.endswith(".json")]

#
#
# THE REAL SHIT
#
#
TRAIN_FILENAMES = BASE_MPD_FILENAMES

CHALLENGE_SET_FILENAME= BASEPATH+"challenge_set.json"


AUX = "aux/"
OUTPUT="entities/"

def reduce_df(out_file):
    
    print("[{}] MERGING {}...".format(str(datetime.datetime.now()), out_file))
    df = pd.concat([pd.read_csv(AUX+f) for f in os.listdir(AUX)])
    df.drop_duplicates().to_csv(OUTPUT+out_file, index=False)
    print("Merged to {}".format(OUTPUT+out_file))

def build_csv(map_chunk_function, out_file, n_chunks, reduce_function=reduce_df):
    try:
        shutil.rmtree(AUX)    
    except:
        pass
    os.mkdir(AUX)
    n_cores = cpu_count()
    print("[{}] {}: Uso {} cores. Splitte en {} por memory issues".format(str(datetime.datetime.now()), out_file, n_cores, n_chunks))
    chunks = np.array_split(TRAIN_FILENAMES, n_chunks)
    pool = Pool(n_cores)
    pool.map(map_chunk_function, chunks)
    reduce_function(out_file)
    shutil.rmtree(AUX)
    print("Deleted {}".format(AUX))
    pool.close()

def load_files(files):
    pls = []
    for f in files:
        pls += json.load(open(f))['playlists']
    try:
        print("=>",end="", flush=True)
    except:
        pass # Empty playlists
    return pls
    
def build_playlists(chunk):
    playlists = load_files(chunk)
    if not playlists:
        return
    for pl in playlists:
        try:
            del pl['tracks']
        except:
            print(pl)
    res = pd.DataFrame(playlists) 
    f = "{}{}.csv".format(AUX, playlists[0]['pid'])
    res.set_index("pid").to_csv(f)
    print("Saved {} for {} files".format(f, len(chunk)))
    

def build_tracks(chunk):
    playlists = load_files(chunk)
    if not playlists:
        return
    
    tracks = []
    for pl in playlists:
        try:
            tracks +=pl['tracks']
        except:
            print(pl['pid'], "no tracks")
    
    
    f = "{}{}.csv".format(AUX, playlists[0]['pid'])
    
    pd.DataFrame(tracks).set_index("track_uri")[['track_name', 'duration_ms']].drop_duplicates().to_csv(f)
    
    #print("Saved {} for {} files".format(f, len(chunk)))
    print(".", end="", flush=True)
    
def build_artists(chunk):
    playlists = load_files(chunk)
    if not playlists:
        return
    tracks = []
    for pl in playlists:
        try:
            tracks +=pl['tracks']
        except:
            print(pl['pid'], "no tracks")
    
    
    f = "{}{}.csv".format(AUX, playlists[0]['pid'])
    
    pd.DataFrame(tracks).set_index("artist_uri")[['artist_name']].drop_duplicates().to_csv(f)
    
    #print("Saved {} for {} files".format(f, len(chunk)))
    print(".", end="", flush=True)
    
def build_albums(chunk):
    playlists = load_files(chunk)
    if not playlists:
        return
    tracks = []
    for pl in playlists:
        try:
            tracks +=pl['tracks']
        except:
            print(pl['pid'], "no tracks")
    
    
    f = "{}{}.csv".format(AUX, playlists[0]['pid'])
    
    pd.DataFrame(tracks).set_index("album_uri")[['album_name']].drop_duplicates().to_csv(f)
    
    #print("Saved {} for {} files".format(f, len(chunk)))
    print(".", end="", flush=True)
    
builds = {
    'albums.csv': build_albums,
    'playlists.csv': build_playlists,
    'artists.csv': build_artists,
    'tracks.csv': build_tracks,
}


def build():
    for csv, map_func in builds.items():
        print("Creating", csv)
        build_csv(map_chunk_function=map_func, out_file=csv, n_chunks=128)

if __name__ == '__main__':
     build()

