
import os,sys
from datetime import datetime, date, timedelta
import pandas as pd
import glob
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
import time
# import feather


class NoDataFoundException(Exception):
    pass

class FileExists(Exception):
    pass

def try_read_csv_df(initfile):
    while True:
        try:
            initdf = pd.read_csv(initfile, index_col=0)
            initdf.index = pd.to_datetime(initdf.index)
            return initdf
        except Exception as e:
            print(e)
            print('retrying')
            time.sleep(1)
            continue

def try_read_parquet(initfile_parquet):
    while True:
        try:
            print('read parquet:{}'.format(initfile_parquet))
            initdf = pd.read_parquet(initfile_parquet,
                                     engine='fastparquet',
                                     )
            print('DONE read parquet:{}'.format(initfile_parquet))
            return initdf
        except Exception as e:
            print(e)
            print('retrying')
            time.sleep(1)
            continue

class myArcticClient(object):
    def __init__(self, dbrootdir):
        self.dbrootdir = dbrootdir
    
    def list_libraries(self, id=-1):
        dbs = list()
        for item in os.listdir(self.dbrootdir):
            if os.path.isdir(os.path.join(self.dbrootdir, item)):
                # print(item)
                dbs.append(item)
        return dbs
    
    def get_library(self, libname):
        return myArctic(os.path.join(self.dbrootdir, libname))


class myArctic(object):
    def __init__(self, dbpath, workers=1):
        self.dbpath = dbpath
        if not os.path.isdir(dbpath):
            raise Exception('{} library not found.'.format(self.dbpath))

        self.workers = workers

    def list_symbols(self):
        syms = list()
        for item in os.listdir(self.dbpath):
            if os.path.isdir(os.path.join(self.dbpath, item)) \
                    and os.path.isfile(os.path.join(self.dbpath,'{}.parquet'.format(item))):
                # print(item)
                syms.append(item)
        return syms

    def read_singleday(self, indicator, dt=datetime(2005,1,1)):
        assert isinstance(dt, datetime) or isinstance(dt, date)
        fin = os.path.join(self.dbpath, indicator, '{}.csv.bz2'.format(dt.strftime('%Y%m%d')))
        try:
            df = pd.read_csv(fin, index_col=0)
            df.index = pd.to_datetime(df.index)
        except:
            raise NoDataFoundException('{} not found'.format(fin))
        return df

    def read_single_wrapper(self, indicator, dt=datetime(2005,1,1), date_range=None, chunk_range=None, columns=None):
        return [indicator, self.read_single_indicator(indicator, dt, date_range, chunk_range, columns)]



    def read(self, indicator, dt=datetime(2005,1,1), date_range=None, chunk_range=None, columns=None):
        # print(locals())
        if isinstance(indicator,str):
            return self.read_single_indicator(indicator,dt,date_range,chunk_range,columns)
        elif isinstance(indicator, list) or isinstance(indicator, tuple):
            data_dict = dict()
            if self.workers > 1:
                with ProcessPoolExecutor(max_workers=self.workers) as exe:
                    plist = [exe.submit(self.read_single_wrapper,indi,dt,date_range, chunk_range, columns)
                             for indi in indicator]
                    ress = [k.result() for k in as_completed(plist)]
                        
            else:
                ress = [self.read_single_wrapper(indi,dt,date_range, chunk_range, columns) for indi in indicator]

            for res in ress:
                # print(res[0])
                # print(res[1])
                data_dict[res[0]] = res[1]
            return data_dict


            # data_dict = dict()
            # for indi in indicator:
            #     data_dict[indi] = self.read_single_indicator(indi,dt,date_range, chunk_range, columns)
            # return data_dict


    def read_single_indicator(self, indicator, dt=datetime(2005,1,1), date_range=None, chunk_range=None, columns=None):
        # print(locals())
        # print(vars())
        if date_range is not None or chunk_range is not None:
            # omit dt parameters
            dt = None
        # alldf = pd.DataFrame()
        # print('param dt:'.format(dt))
        if dt is not None:
            alldf = self.read_singleday(indicator, dt)
        else:
            dt_range = None
            if date_range is not None:
                # print('set date_range')
                dt_range = date_range
            elif chunk_range is not None:
                # print('set chunk_range')
                dt_range = chunk_range
            initdf = pd.DataFrame()
            initfile = os.path.join(self.dbpath,'{}.csv.bz2'.format(indicator))
            initfile_parquet = os.path.join(self.dbpath,'{}.parquet'.format(indicator))
            if os.path.isfile(initfile_parquet):
                initdf = try_read_parquet(initfile_parquet)
                # initdf.index = pd.to_datetime(initdf.index)

            elif os.path.isfile(initfile):
                initdf = try_read_csv_df(initfile)
                # initdf = pd.read_csv(initfile, index_col=0)
                # initdf.index = pd.to_datetime(initdf.index)

            # calendar days
            cal_days = pd.date_range(dt_range[0],dt_range[1],freq='D')

            t = time.time()

            allfps = glob.glob(os.path.join(self.dbpath, indicator,'*.csv.bz2'))
            allfns = [os.path.basename(item).split('.')[0] for item in allfps]
            my_tdays = [datetime.strptime(item,'%Y%m%d') for item in allfns]
            all_tdays = set(cal_days) & set(my_tdays)
            # print('t:{}'.format(time.time() - t))
            # t = time.time()

            # not_in_initdf_index = set(all_tdays) - set(initdf.index.unique())
            print('indicator:{}'.format(indicator))
            not_in_initdf_index = set(all_tdays) - set(pd.to_datetime(pd.Series(sorted(list(set(initdf.index.date))))))
            # print('t:{}'.format(time.time() - t))
            # t = time.time()
            print('not_in_initdf_index: len:{}'.format(len(not_in_initdf_index)))
            dfs = list()
            for tday in sorted(not_in_initdf_index):
                print(tday, type(tday))
                today_close = self.read_singleday(indicator, tday)
                dfs.append(today_close)
            if len(dfs) > 0:
                adddf = pd.concat(dfs)
                adddf.index = pd.to_datetime(adddf.index)
                alldf = initdf.append(adddf)
                alldf = alldf.sort_index().truncate(before=dt_range[0],after=dt_range[1])
            else:
                alldf = initdf.sort_index().truncate(before=dt_range[0],after=dt_range[1])
        ######################
        if columns is not None:
            alldf = alldf[columns]

        return alldf


    def write(self, indicator, dt, df, force=False):
        fout = os.path.join(self.dbpath, indicator, '{}.csv.bz2'.format(dt.strftime('%Y%m%d')))
        # if os.path.isfile(fout) and not force:
        #     raise FileExists('{} exists'.format(fout))
        if not os.path.isdir(os.path.dirname(fout)):
            os.makedirs(os.path.dirname(fout))
        df.to_csv(fout, compression='bz2')

    def update_initfile(self, indicator):
        '''
        update initfile to now - 5days
        :param indicator:
        :return:
        '''
        df = self.read(indicator,chunk_range=[datetime(2005,1,1), datetime.now() - timedelta(days=5)])
        fout = os.path.join(self.dbpath, '{}.parquet'.format(indicator))
        try:
            df = df.astype(pd.np.float32)
        except:
            pass
        df.to_parquet(fout)

        # fout_feather = os.path.join(self.dbpath, '{}.feather'.format(indicator))
        # feather.write_dataframe(df, fout_feather, version=1)


    def update_all_indicators_initfile(self):
        indicators = self.list_symbols()
        for indicator in sorted(indicators):
            print('update initfile for {}'.format(indicator))
            self.update_initfile(indicator)

