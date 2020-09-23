# Arctic_local

## local panel data storage using parquet,api compatible with Man AHL's Arctic .
## most suitable for stock backtesting system.


like the project "pystore", data stored in a directory:
base data file:
{dbdatadir}/{library_name}/{indicator}.parquet
incremental data file every day:
{dbdatadir}/{library_name}/{indicator}/{yyyy-mm-dd}.tar.bz2

data file content is a pandas dataframe, indexed by a DateTimeIndex type:
columns: symbols or any other fields you want .
index: dates/datetime for min1 data
2005-01-04,

we can update basedata file every week/day after trading.

