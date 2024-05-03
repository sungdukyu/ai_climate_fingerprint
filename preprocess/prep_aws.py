import s3fs
import xarray as xr
import numpy as np
from os import path, system
import sys
from datetime import date
import pandas as pd

# parallel -j 50 --delay 1 --joblog ./process_logs/progress.parallel.log python prep_aws.py ::: `cat zstore_to_download.2024-05-02.csv`

def read_args():
    if len(sys.argv) == 2:
        argument = sys.argv[1]
        return argument
    else:
        print("No or too many command line argument(s).")
        return None
    
def download_aws_cmip_table(activity_id=['CMIP', 'ScenarioMIP'], experiment_id=['historical', 'ssp370'], variable_id=['tas', 'huss', 'pr'], table_id=['day']):
    # download the latest version of the pangeo-cmip6.csv
    # usage: $ python -c 'from prep_aws import download_aws_cmip_table; download_aws_cmip_table()'

    # download
    f_save = f'pangeo-cmip6.aws.{str(date.today())}.csv'
    system(f'wget https://cmip6-pds.s3.amazonaws.com/pangeo-cmip6.csv -O pangeo-cmip6.csv; mv pangeo-cmip6.csv {f_save}; touch {f_save}');
    print(f'Downloaded: {f_save}')

    # query the csv file and save zstore address
    cmip_aws = pd.read_csv(f_save)
    tmp_query = f"activity_id=={activity_id} &\
                  experiment_id=={experiment_id} &\
                  table_id=={table_id} &\
                  variable_id=={variable_id} \
                  "
    tmp = cmip_aws.query(tmp_query)
    tmp = tmp['zstore']
    f_save = f'zstore.{'-'.join(activity_id)}.{'-'.join(experiment_id)}.{'-'.join(table_id)}.{'-'.join(variable_id)}.{str(date.today())}.csv'
    tmp.to_csv(f_save,header=None,index=None)
    print(f'Saved: {f_save}')

def preprocess(f_in:str, 
               subsample:int=0, 
               regrid=True, re_lat=np.arange(-90+2.8125*.5,90,2.8125), re_lon=np.arange(2.8125*.5,360,2.8125),
               fp='float32'
               ):
    # open a source zarr store
    fs = s3fs.S3FileSystem(anon=True)
    mapper = fs.get_mapper(f_in)
    prepdata = xr.open_zarr(mapper, consolidated=True)

    # take the variable name
    varname = f_in.split('/')[10]

    # get rid of auxillary variables
    prepdata = prepdata[varname]

    # convert calendar
    if False:
        match prepdata.time.dt.calendar:
            case 'noleap':
                pass
            case '360_day':
                # prepdata = prepdata.convert_calendar('noleap', align_on='year')
                pass
            case other:
                # prepdata = prepdata.convert_calendar('noleap')
                pass
    
    # get rid of dates out of range
    exp = f_in.split('/')[7]
    if exp == 'historical':
        prepdata = prepdata.sel(time=slice("1850-01-01","2014-12-31"))
    if exp[:3] == 'ssp':
        # TODO
        prepdata = prepdata.sel(time=slice("2015-01-01","2100-12-31"))
        pass
    # TODO: else: raise value error
    
    # sub sampling
    if subsample > 0:
        sfreq = f'{subsample}D'
        prepdata = prepdata.resample(time=sfreq).nearest()

    # interpolate
    if regrid:
        prepdata = prepdata.interp(lat=re_lat, lon=re_lon,
                                   method='linear',
                                   kwargs={'fill_value': 'extrapolate'} # TODO: check the implication of extrapolation
                                   )

    # convert to single precision
    prepdata = prepdata.astype(fp)
    

    return prepdata

def main():
    # read arg
    f_src = read_args()
    # f_src = 's3://cmip6-pds/CMIP6/CMIP/AS-RCEC/TaiESM1/1pctCO2/r1i1p1f1/Amon/tas/gn/v20200225/' # for debugging

    # f_dst
    f_dst_prefix = '/export/work/sungduky/climate/fingerprint_dataset/preprocessed'
    f_dst = f_src.replace('s3://cmip6-pds/','').replace('/','_')
    f_dst = f'{f_dst_prefix}/{f_dst[:-1]}.nc'

    if path.isfile(f_dst):
        print(f'File already exists: {f_dst}')
        return
    else:

        # preprocess (regrid / reduce precision)
        to_save = preprocess(f_src, regrid=True, fp='float32')

        # save annual mean
        to_save2 = to_save.resample(time='1YE').mean()
        to_save2['time'] = to_save2.time.dt.year
        to_save2.to_netcdf(f_dst[:-3]+'.annual.nc')
        print(f'Saved (annual):\t{f_dst[:-3]+'.annual.nc'}')

        # subsample (5 day) and daily data    
        to_save = to_save.resample(time='5D').nearest()
        to_save.to_netcdf(f_dst, encoding={'time': {'dtype':'float64'}})
        print(f'Saved (daily):\t{f_dst}')

        return

if __name__ == '__main__':
    main()
