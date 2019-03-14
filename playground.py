import pandas as pd
from haversine import haversine
from datetime import datetime
from joblib import Parallel, delayed
from multiprocessing import Pool, cpu_count

def deltaDist(x):
    return haversine((float(x["lat"]), float(x["lon"])),(float(x["lat2"]), float(x["lon2"])))

def deltaTime(x):
    return abs(
            datetime.strptime(x["time_stamp"], "%Y-%m-%d %H:%M:%S") - datetime.strptime(x["time_stamp2"], "%Y-%m-%d %H:%M:%S")
            ).seconds/3600

def calDelta(df):
    print("Enter..")
    df.drop_duplicates(subset=["unit_id", "lat", "lon", "speed", "unit_type"], keep="last", inplace=True)
    if len(df) > 1:
        shifted = df.shift(1).rename(index=int,\
                columns={"time_stamp":"time_stamp2", "unit_id":"unit_id2", "lat":"lat2", "lon":"lon2", "speed":"speed2"})

        concated = pd.concat([df, shifted], axis=1, sort=False).iloc[1:,:]
        concated.drop(["unit_id2", "unit_type"], axis=1, inplace=True)

        concated["delta_dist"] = concated[["lat", "lon", "lat2", "lon2"]]\
            .apply(lambda x: deltaDist(x), axis=1)

        concated["delta_time"] = concated[["time_stamp", "time_stamp2"]]\
            .apply(lambda x: deltaTime(x) ,axis=1)
        concated = concated.loc[concated["delta_time"] <= 0.083] # Less than or equal to 5 minutes
    return concated

def applyParallel(dfGrouped, func):
    retLst = Parallel(n_jobs=cpu_count())(delayed(func)(group) for name, group in dfGrouped)
    return pd.concat(retLst)

if __name__ == '__main__':
    import time
    t = time.time()
    dataset = pd.read_csv("road#3504.csv")
    groups = dataset.groupby("unit_id")
    result = applyParallel(groups, calDelta)
    result.to_csv("result.csv")
    print(time.time() - t)