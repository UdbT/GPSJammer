import pandas as pd
from joblib import Parallel, delayed
import multiprocessing

def tmpFunc(df):
    df['c'] = df.a + df.b
    return df

def applyParallel(dfGrouped, func):
    retLst = Parallel(n_jobs=multiprocessing.cpu_count())(delayed(func)(group) for name, group in dfGrouped)
    return pd.concat(retLst)

if __name__ == '__main__':
    df = pd.DataFrame({'a': [6, 2, 2], 'b': [4, 5, 6]},index= ['g1', 'g1', 'g2'])
    print ('parallel version: ')
    print (applyParallel(df.groupby(df.index), tmpFunc))

    print ('regular version: ')
    print (df.groupby(df.index).apply(tmpFunc))

    print ('ideal version (does not work): ')
    print (df.groupby(df.index).applyParallel(tmpFunc))