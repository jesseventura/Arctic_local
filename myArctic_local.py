
import os,sys
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import glob
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
import time
# import feather
from progressbar import ProgressBar
import shutil
from pyarrow.parquet import read_schema


def read_csv_sfp(fp):
    df = pd.DataFrame()
    try:
        df = pd.read_csv(fp,index_col=0)
    except Exception as e:
        print(fp)
#     try:
#         df = df.astype(np.float32)
#     except:
#         print('{} cannot read'.format(fp))
    return df


def combine_single_indicator(subdir, indicator, force, kw):
    fout = os.path.join(subdir, '{}.parquet'.format(indicator))
    if os.path.isfile(fout) and not force:
        return
    print(fout)

    wkn = 5
    # if kw.fdd = 1

    print('start in workers: {}'.format(wkn))
    # time.sleep(2)
    with ProcessPoolExecutor(max_workers=wkn) as exe:
        plist = list()
        dfs = list()
        #             dfall = pd.DataFrame()
        for fp in (sorted(glob.glob(os.path.join(subdir, indicator, '*.csv.bz2')))):
            tday_str = os.path.basename(fp).split('.csv')[0]
            if len(tday_str) != 8:
                continue
            #                 print(tday_str)
            # if day > a week ago
            # if tday_str > (datetime.now() - timedelta(days=5)).strftime('%Y%m%d'):
            #     continue
            plist.append(exe.submit(read_csv_sfp, fp))
        for k in ProgressBar(max_value=len(plist))(as_completed(plist)):
            df = k.result()
            dfs.append(df)
    #                 dfall = dfall.append(df)

    #             df = pd.read_csv(fp,index_col=0)
    #             dfs.append(df)
    #             df = df.append()
    if len(dfs) < 1:
        return
    print(f'concat DFS !!!!!!!!!! {indicator} {fout}')
    df = pd.concat(dfs,copy=False)
    #         df = dfall
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df.to_parquet(fout)


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

def try_read_parquet(initfile_parquet, columns=None):
    count = 0
    while True:
        try:
            # print('read parquet:{}'.format(initfile_parquet))
            sch = read_schema(initfile_parquet)
            # cols_valid = [item for item in sch.names if item.find('.XSH')>0 ]
            if columns is not None:
                columns = list(set(sch.names) & set(columns))
            initdf = pd.read_parquet(initfile_parquet,
                                     # engine='fastparquet',
                                     columns=columns,
                                     )
            # print('DONE read parquet:{}'.format(initfile_parquet))
            return initdf
        except Exception as e:
            count += 1
            if count > 10:
                break
            print(e)
            print('retrying')
            time.sleep(1)
            continue

class myArcticClient(object):
    def __init__(self, dbrootdir, ):
        self.dbrootdir = dbrootdir

    def list_libraries(self, id=-1):
        dbs = list()
        for item in os.listdir(self.dbrootdir):
            if os.path.isdir(os.path.join(self.dbrootdir, item)):
                # print(item)
                dbs.append(item)
        return dbs

    def get_library(self, libname, workers=1):
        return myArctic(os.path.join(self.dbrootdir, libname), workers)

    def initialize_library(self, libname):
        libpath = os.path.join(self.dbrootdir, libname)
        if os.path.isdir(libpath):
            pass
        else:
            os.makedirs(libpath)


class myArctic(object):
    def __init__(self, dbpath, workers=1):
        self.dbpath = dbpath
        if not os.path.isdir(dbpath):
            raise Exception('{} library not found.'.format(self.dbpath))

        self.workers = workers
        self.libname = os.path.basename(self.dbpath)

    def list_symbols(self):
        syms = list()
        for item in os.listdir(self.dbpath):
            if os.path.isdir(os.path.join(self.dbpath, item)) \
                    and os.path.isfile(os.path.join(self.dbpath,'{}.parquet'.format(item))):
                # print(item)
                syms.append(item)
        return syms

    def read_singleday(self, indicator, dt=datetime(2005,1,4)):
        assert isinstance(dt, datetime) or isinstance(dt, date)
        fin = os.path.join(self.dbpath, indicator, '{}.csv.bz2'.format(dt.strftime('%Y%m%d')))
        try:
            df = pd.read_csv(fin, index_col=0)
            df.index = pd.to_datetime(df.index)
        except:
            raise NoDataFoundException('{} not found'.format(fin))
        return df

    def read_single_wrapper(self, indicator, dt=datetime(2005,1,4), date_range=None, chunk_range=None, columns=None,use_initfile=True):
        return [indicator, self.read_single_indicator(indicator, dt, date_range, chunk_range, columns,use_initfile)]



    def read(self, indicator, dt=datetime(2005,1,4), date_range=None, chunk_range=None, columns=None,use_initfile=True):
        # print(locals())
        if isinstance(indicator,str):
            return self.read_single_indicator(indicator,dt,date_range,chunk_range,columns,use_initfile)
        elif isinstance(indicator, list) or isinstance(indicator, tuple):
            data_dict = dict()
            if self.workers > 1:
                with ProcessPoolExecutor(max_workers=self.workers) as exe:
                    plist = [exe.submit(self.read_single_wrapper,indi,dt,date_range, chunk_range, columns)
                             for indi in indicator]
                    ress = [k.result() for k in ProgressBar(max_value=len(plist))(as_completed(plist))]

            else:
                ress = [self.read_single_wrapper(indi,dt,date_range, chunk_range, columns,use_initfile) for indi in ProgressBar()(indicator)]

            for res in ress:
                # print(res[0])
                # print(res[1])
                data_dict[res[0]] = res[1]
            return data_dict


            # data_dict = dict()
            # for indi in indicator:
            #     data_dict[indi] = self.read_single_indicator(indi,dt,date_range, chunk_range, columns)
            # return data_dict


    def read_single_indicator(self, indicator, dt=datetime(2005,1,4), date_range=None, chunk_range=None,
                              columns=None,use_initfile=True):
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
            if os.path.isfile(initfile_parquet) and use_initfile:
                initdf = try_read_parquet(initfile_parquet,columns=columns)
                # initdf.index = pd.to_datetime(initdf.index)

            elif os.path.isfile(initfile) and use_initfile:
                initdf = try_read_csv_df(initfile)
                # initdf = pd.read_csv(initfile, index_col=0)
                # initdf.index = pd.to_datetime(initdf.index)
            elif not use_initfile:
                initdf = pd.DataFrame()

            # calendar days
            cal_days = pd.date_range(dt_range[0].date(),dt_range[1].date(),freq='D')

            t = time.time()

            # allfps = glob.glob(os.path.join(self.dbpath, indicator,'*.csv.bz2'))
            allfps = [item for item in os.listdir(os.path.join(self.dbpath, indicator)) if item.endswith('.csv.bz2')]
            allfns = [os.path.basename(item).split('.')[0] for item in allfps]
            my_tdays = list()
            for item in allfns:
                try:
                    my_tdays.append(datetime.strptime(item,'%Y%m%d'))
                except:
                    continue
            # my_tdays = [datetime.strptime(item,'%Y%m%d') for item in allfns]
            all_tdays = set(cal_days) & set(my_tdays)
            # print(all_tdays)
            # print('t:{}'.format(time.time() - t))
            # t = time.time()

            # not_in_initdf_index = set(all_tdays) - set(initdf.index.unique())
            # print('indicator:{}'.format(indicator))
            try:
                # print(initdf.index)
                if initdf.shape[0] < 1:
                    not_in_initdf_index = set(all_tdays)
                else:
                    not_in_initdf_index = set(all_tdays) - set(pd.to_datetime(pd.Series(sorted(list(set(initdf.index.date))))))
                # print(not_in_initdf_index)
            except Exception as e:
                print('indicator:{}'.format(indicator))
                raise Exception(e)
            # print('t:{}'.format(time.time() - t))
            # t = time.time()
            # print('not_in_initdf_index: len:{}'.format(len(not_in_initdf_index)))
            dfs = list()
            for tday in sorted(not_in_initdf_index):
                # print(tday, type(tday))
                today_close = self.read_singleday(indicator, tday)
                if columns is not None:
                    today_close = today_close.reindex(columns=columns)
                dfs.append(today_close)
            if len(dfs) > 0:
                adddf = pd.concat(dfs,copy=False)
                adddf.index = pd.to_datetime(adddf.index)
                alldf = pd.concat([initdf,adddf],copy=False)
                # TODO: miniute must include 23hours
                alldf = alldf.sort_index().truncate(before=dt_range[0],after=dt_range[1])
            else:
                alldf = initdf.sort_index().truncate(before=dt_range[0],after=dt_range[1])
        ######################
        if columns is not None:
            alldf = alldf[columns]

        return alldf


    def write(self, indicator, dt=datetime(2005,1,4), df=pd.DataFrame(), force=False):
        fout = os.path.join(self.dbpath, indicator, '{}.csv.bz2'.format(dt.strftime('%Y%m%d')))
        # if os.path.isfile(fout) and not force:
        #     raise FileExists('{} exists'.format(fout))
        if not os.path.isdir(os.path.dirname(fout)):
            os.makedirs(os.path.dirname(fout))
        df.to_csv(fout, compression='bz2')

    def update_initfile(self, indicator, days_before=0, ):
        '''
        update initfile to now - 5days
        :param indicator:
        :return:
        '''
        df = self.read(indicator,chunk_range=[datetime(2005,1,4), datetime.now() - timedelta(days=days_before)])
        fout = os.path.join(self.dbpath, '{}.parquet'.format(indicator))
        try:
            df = df.astype(np.float32)
        except:
            pass
        # save write
        df.to_parquet(fout+'.1')
        shutil.move(fout+'.1',fout,)

        # fout_feather = os.path.join(self.dbpath, '{}.feather'.format(indicator))
        # feather.write_dataframe(df, fout_feather, version=1)

    def update_single_indicator(self, indicator, days_before=0, ):
        print('update initfile for {}'.format(indicator))
        try:
            self.update_initfile(indicator, days_before, )
        except:
            pass

    def update_all_indicators_initfile(self, days_before=0, ):
        with ProcessPoolExecutor(max_workers=self.workers) as exe:
            plist = [exe.submit(self.update_single_indicator,indicator,days_before,) for indicator in sorted(self.list_symbols())]
            res = [r.result() for r in ProgressBar(max_value=len(plist))(as_completed(plist))]


    def create_init_files(self, force_recreate=False):
        '''

        :param force_recreate: 要不要重新生成parquet文件
        :return:
        '''

        subdir = self.dbpath
        # factor category as db
        if not os.path.isdir(subdir):
            return
        wkn = self.workers
        if self.libname.find('min1') >=0 and self.libname.find('Block') <0:
            wkn = 1
        plist = list()
        with ProcessPoolExecutor(max_workers=wkn) as exe:
            for indicator in (sorted(os.listdir(subdir))):
                # factorname as subdir name
                print('----',indicator)
                if not os.path.isdir(os.path.join(subdir, indicator)):
                    continue
                plist.append(exe.submit(combine_single_indicator, subdir, indicator, force_recreate, self.libname))
            for k in ProgressBar(max_value=len(plist))(as_completed(plist)):
                pass

