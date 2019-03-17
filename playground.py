import pandas as pd
from haversine import haversine
from datetime import datetime
from joblib import Parallel, delayed
from multiprocessing import Pool, cpu_count
import time
def deltaDist(x):
    return haversine((float(x["lat"]), float(x["lon"])),(float(x["lat2"]), float(x["lon2"])))

def deltaTime(x):
    return abs(
            datetime.strptime(x["time_stamp"], "%Y-%m-%d %H:%M:%S") - datetime.strptime(x["time_stamp2"], "%Y-%m-%d %H:%M:%S")
            ).seconds/3600

def calDelta(df):
    # print(df[1])
    df[0].drop_duplicates(subset=["unit_id", "lat", "lon", "speed", "unit_type"], keep="last", inplace=True)
    if len(df[0]) > 1:
        shifted = df[0].shift(1).rename(index=int,\
                columns={"time_stamp":"time_stamp2", "unit_id":"unit_id2", "lat":"lat2", "lon":"lon2", "speed":"speed2"})

        concated = pd.concat([df[0], shifted], axis=1, sort=False).iloc[1:,:]
        concated.drop(["unit_id2", "unit_type"], axis=1, inplace=True)

        concated["delta_dist"] = concated[["lat", "lon", "lat2", "lon2"]]\
            .apply(lambda x: deltaDist(x), axis=1)

        concated["delta_time"] = concated[["time_stamp", "time_stamp2"]]\
            .apply(lambda x: deltaTime(x) ,axis=1)
        concated = concated.loc[concated["delta_time"] <= 0.083] # Less than or equal to 5 minutes
        return concated

def applyParallel(dfGrouped, func):
    with Pool(cpu_count()) as p:
        ret_list = p.map(func, [(group, name) for name, group in dfGrouped])
    return pd.concat(ret_list)

if __name__ == '__main__':
    import time
    dataset = pd.read_csv("road#3504.csv")
    groups = dataset.groupby("unit_id")
    t = time.time()
    # print(len(groups))
    # result = []
    # for name, group in groups:
    #     result.append(calDelta((group, name)))
    # result = pd.concat(result)  
    result = applyParallel(groups, calDelta)
    print(time.time() - t)

    result.to_csv("result.csv")