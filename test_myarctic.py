from myArctic_local import myArcticClient
from datetime import datetime

cli = myArcticClient('/mnt/Data/StockDB')
for libname in cli.list_libraries():
    print(libname)
    mylib = cli.get_library(libname)
    indicators = mylib.list_symbols()
    df = mylib.read(indicators[0], date_range=(datetime(2005,1,1),datetime(2005,2,1)),columns=['000001.XSHE','000300.XSHG'])
    df_dict = mylib.read(indicators[:3],datetime(2005,1,4))
    df_dict2 = mylib.read(indicators[:3],date_range=(datetime(2005,1,1),datetime(2005,2,1)))
    
    print(df)
    print(df_dict)
    print(df_dict2)
    break

# custom factors library
# custom_factors_cli = myArcticClient('/mnt/Data/StockDB/custom_factors/')
        
