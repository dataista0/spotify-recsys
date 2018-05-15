import os
import json
import pandas as pd
import numpy as np
import multiprocessing

BASEPATH='data/mpd/reduced/'
OUT='data/mpd/csv/'

def process_file(fp):
    all_dfs = []
    with open(fp) as f:
        data = json.load(f)
        for playlist in data:
            interactions = pd.DataFrame(playlist['tracks'], columns=['track'])
            interactions.loc[:, 'playlist'] = playlist['pid']
            all_dfs.append(interactions)
    return all_dfs

def process_files(files):
    print("Processing", len(files), "files")
    all_dfs = []
    i = 0
    i_no_reset = 0
    for index, name, fullpath in files:
        all_dfs += process_file(fullpath)
        print("{}({})".format(i_no_reset, index), end="\t", flush=True)
        if i > 50:
            print("DUMPING", index)
            pd.concat(all_dfs).to_csv(OUT+str(index)+".csv", index=False)
            all_dfs = []
            i=0
        i+=1
        i_no_reset+=1
    
    if all_dfs:
        print("DUMPING", index)
        pd.concat(all_dfs).to_csv(OUT+str(index)+".csv", index=False)


def run():
    files = [(i, f, BASEPATH + f) for i, f in enumerate(os.listdir(BASEPATH)) if f.endswith(".json") and f.startswith("mpd")]
    num_threads = 4
    pool = multiprocessing.Pool(num_threads)
    files_splits = np.array_split(files, num_threads)

    pool.map(process_files, files_splits)

if __name__ == '__main__':
    run()
