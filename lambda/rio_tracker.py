import numpy as np
import xarray as xr
import datetime
import pandas as pd
import fsspec

from wurlitzer import pipes
from contextlib import contextmanager
import sys, os

from weather_router import isochronal_weather_router, polar, point_validity

import geopy.distance
import json

import ctypes 

import holoviews as hv
import geoviews as gv
import hvplot.xarray
import cartopy.crs as ccrs
from bokeh.resources import INLINE

@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:  
            yield
        finally:
            sys.stdout = old_stdout

def return_parsed():
    fs = fsspec.filesystem('https')
    f = fs.open("https://cf.yb.tl/BIN/c2r-2023/AllPositions3", "rb")
    pos_bytes = f.read()
    so_file = "decyb.so"
    lib = ctypes.CDLL(so_file)
    lib.PositionParser.argtypes = [ctypes.c_char_p, ctypes.c_int]
    lib.PositionParser.restype = ctypes.c_int
    return lib.PositionParser(pos_bytes, len(pos_bytes))


def handler(event, context):
    object_key = event["Records"][0]["s3"]["object"]["key"]
    fs = fsspec.filesystem('s3')
    print(f"s3://riotrackerlambdastack-cape2riotrackingbucket493cd-ax0w0veyvbks/{object_key}")
    with fs.open(f"s3://riotrackerlambdastack-cape2riotrackingbucket493cd-ax0w0veyvbks/{object_key}", 'rb') as f:
        input_data = json.load(f)

    name = input_data['boat']
    file_name = '_'.join(name.split(' '))

    #get tracks
    with suppress_stdout():
        with pipes() as (out, err):
            _ = return_parsed()

    tracks = eval(out.read())
    print('tracks')
    #get_leaderboard
    fs = fsspec.filesystem('https')
    with fs.open("https://cf.yb.tl/JSON/c2r-2023/leaderboard", "r") as f:
        leaderboard = eval(f.read().replace('false','False').replace('true',"True").replace('null',"None"))
    print('leaderboard')
    #get race setup
    with fs.open("https://cf.yb.tl/JSON/c2r-2023/RaceSetup", "r") as f:
        RaceSetup = eval(f.read().replace('false','False').replace('true',"True").replace('null',"None"))
    print('RaceSetup')
    data = {}
    for i,x in enumerate(RaceSetup['teams']):
        data[x['name']] = {}
        data[x['name']]['handicap'] = RaceSetup['teams'][i]['tcf1']
        data[x['name']]['status'] = RaceSetup['teams'][i]['status']
        data[x['name']]['model'] = RaceSetup['teams'][i]['model']
        data[x['name']]['country'] = RaceSetup['teams'][i]['country']
        data[x['name']]['captain'] = RaceSetup['teams'][i]['captain']
        data[x['name']]['colour'] = RaceSetup['teams'][i]['colour']
        data[x['name']]['position'] = (tracks[i]['moments'][0]['lat'], tracks[i]['moments'][0]['lon'])
        data[x['name']]['time'] = datetime.datetime.fromtimestamp(tracks[i]['moments'][0]['at'])
        data[x['name']]['track'] = tracks[i]['moments']

    #parameters
    year = data[name]['time'].year
    month = data[name]['time'].month
    day = data[name]['time'].day
    hour = data[name]['time'].day - 2 #correct to utc
    max_days = 12
    boat_pos = data[name]['position']
    start = (RaceSetup['course']['nodes'][0]['lat'], RaceSetup['course']['nodes'][0]['lon'])
    end = (RaceSetup['course']['nodes'][-1]['lat'], RaceSetup['course']['nodes'][-1]['lon'])
    step = 3
    extent = [-5, 25, -40, -50]
    spread = 140
    wake_lim = 35
    rounding = 1

    #weatherdata
    ref_url = 's3://lambdagfsreferencestack-gfsreference01a4696a-1lywfe3wpr52o/references/latest.json'
    fs_ = fsspec.filesystem("reference", fo=ref_url, remote_protocol='s3', remote_options={'anon':True})
    m = fs_.get_mapper("")
    ds = xr.open_dataset(m, engine="zarr", backend_kwargs=dict(consolidated=False))
    print('weather fetched')
    ds.coords['longitude'] = ((ds.coords['longitude'] + 180) % 360) - 180
    ds = ds.sortby(ds.longitude)
    ds = ds.drop('time').drop('step').drop('heightAboveGround')
    ds = ds.rename({'valid_time':'time'})
    ds = ds.rename({'latitude':'lat'})
    ds = ds.rename({'longitude':'lon'})
    lat1,lon1,lat2,lon2 = extent
    ds = ds.sel(lat = slice(max([lat1, lat2]),min([lat1, lat2]))).sel(lon = slice(min([lon1, lon2]),max([lon1, lon2])))
    ds = ds.sel(time = slice(f'{year}-{month}-{day}-{hour}:00:00','2100-01-01-12:00:00'))
    ds = ds.sel(time = ds.time.values[:max_days*8])
    #ds = ds.sel(time = ds.time.values[:1*8])
    tws = np.sqrt(ds.v10**2 + ds.u10**2)
    tws = tws*1.94384
    twd = np.mod(180+np.rad2deg(np.arctan2(ds.u10, ds.v10)),360)
    ds_ = tws.to_dataset(name = 'tws')
    ds_['twd'] = twd
    ds_['u10'] = ds.u10*1.94384
    ds_['v10'] = ds.v10*1.94384
    ds_['wind_angle'] = np.deg2rad((270 - (ds_.twd)) % 360)
    del ds_.lat.attrs['units']
    ds_.lon.attrs['long_name'] = 'longitude'
    ds = ds_.load()

    def getWindAt(t, lat, lon):
        tws_sel = ds.tws.sel(time = t, method = 'nearest')
        tws_sel = tws_sel.sel(lat = lat, lon = lon, method = 'nearest')
        twd_sel = ds.twd.sel(time = t, method = 'nearest')
        twd_sel = twd_sel.sel(lat = lat, lon = lon, method = 'nearest')
        return (np.float32(twd_sel.values), np.float32(tws_sel.values))

    #polar set_up
    fs = fsspec.filesystem('s3')
    with fs.open(f"s3://riotrackerlambdastack-cape2riotrackingbucket493cd-ax0w0veyvbks/polars/{file_name}.pol", 'r') as f:
        df = pd.read_csv(f, sep = ';') #move to s3
    df = df.set_index(df.iloc[:,0]).iloc[:,1:]
    df.columns = df.columns.astype(float)
    df.index = df.index.astype(float)
    df.insert(0, 0, pd.Series([0,0,0,0,0,0,0,0,0], index=df.index))
    pol = polar.Polar(df = df)
    print('polars')

    #routing setup
    point_valid = point_validity.land_sea_mask(extent).point_validity_arr
    weatherrouter = isochronal_weather_router.weather_router(pol, 
                        getWindAt, 
                        ds.time.values,
                        step,
                        boat_pos,
                        end,
                        spread = spread,
                        wake_lim = wake_lim,
                        rounding = rounding,
                        point_validity = point_valid)

    #run routing
    weatherrouter.route()
    print('routed')

    route_df = weatherrouter.get_fastest_route()

    ds = ds.sel(time = ds.time.values[:len(route_df)])

    def get_current_lon_lat(time):
        now = route_df.loc[time]
        return gv.Points({'lon': [now.lon], 'lat':[now.lat], 'TWS':[round(now.tws)], 'TWD':[round(now.twd)], 'TWA':[round(now.twa)], 'Boat Speed':[round(now.boat_speed)]}, kdims = ['lon', 'lat'],vdims = ['TWS','TWD','TWA','Boat Speed']).opts(color = 'white', size = 12, tools = ['hover'])
    
    wind = ds['tws'].hvplot(groupby = 'time', geo = True, tiles = 'OSM',alpha = 0.5, cmap = 'jet', clim=(0,30), hover=False)
    dsv = ds.coarsen({'lat':8, 'lon': 8}, boundary='pad').mean()
    vector = dsv.hvplot.vectorfield(x='lon', y='lat', angle='wind_angle', mag='tws', hover=False, groupby = 'time', geo = True).opts(magnitude='tws')

    sample_points = dict(Longitude = route_df.lon.values,Latitude  = route_df.lat.values)
    route = gv.Path(sample_points).opts(color = 'white',line_width=4)

    lats = []
    lons = []
    for point in data[name]['track']:
        lats.append(point['lat'])
        lons.append(point['lon'])

    sample_points2 = dict(Longitude = np.array(lons),Latitude  = np.array(lats))
    route2 = gv.Path(sample_points2).opts(color = 'grey',line_width=4)

    start = gv.Points({'lon': [start[1]], 
                    'lat':[start[0]]}, 
                    kdims = ['lon', 'lat']).opts(color = 'green', 
                                                size = 8, 
                                                tools = ['hover'])

    finish = gv.Points({'lon': [end[1]], 
                    'lat':[end[0]]}, 
                    kdims = ['lon', 'lat']).opts(color = 'red', 
                                                size = 8, 
                                                tools = ['hover'])

    current_point = hv.DynamicMap(get_current_lon_lat, kdims='time')

    plot = (wind*vector*start*finish*route*current_point*route2)
    
    for boat in data:
        boat_point = gv.Points({'lon': [data[boat]['position'][1]], 
                                'lat':[data[boat]['position'][0]],}, 
                                kdims = ['lon', 'lat']).opts(color = f"#{data[boat]['colour']}", 
                                                            size = 8, 
                                                            tools = ['hover'],
                                                            active_tools=['wheel_zoom', 'pan'])
        plot = plot * boat_point
    
    plot = plot.opts(fontscale=1, width=900, height=600)
    hv.output(widget_location='bottom')
    print('plotted')

    
    with fs.open(f"s3://riotrackerlambdastack-cape2riotrackingbucket493cd-ax0w0veyvbks/results/{file_name}.html", 'w') as f:
        hvplot.save(plot, f, resources=INLINE)

    with fs.open(f"s3://riotrackerlambdastack-cape2riotrackingbucket493cd-ax0w0veyvbks/results/{file_name}.csv", 'w') as f:
        route_df.to_csv(f)

    def get_eta_from_last(route_df):
        dist_left = geopy.distance.great_circle(route_df.iloc[-1].pos, end).nm
        speed = pol.getSpeed(route_df.iloc[-1].tws,np.abs(route_df.iloc[-1].twa))
        if speed <1.5:
            speed = 1.5
        return dist_left/speed

    get_eta_from_last(route_df)

    eta = route_df.index[-1] + datetime.timedelta(hours = get_eta_from_last(route_df))

    elapsed = eta - datetime.datetime(2023,1,2,12)

    elapsed_seconds = elapsed.days*(24*60*60)+elapsed.seconds
    elapsed_seconds_corrected = elapsed_seconds*np.float64(data[name]['handicap'])
    corrected = str(datetime.timedelta(seconds = elapsed_seconds_corrected))

    estimates = {}

    estimates['name'] = name
    estimates['eta'] = str(eta)
    estimates['ORC'] = data[name]['handicap']
    estimates['elapsed'] = str(elapsed)
    estimates['corrected'] = str(corrected)

    with fs.open(f"s3://riotrackerlambdastack-cape2riotrackingbucket493cd-ax0w0veyvbks/results/{file_name}.json", 'w') as f:
        json.dump(estimates, f)
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/plain'
        },
        'body': '{} request completed successfully \n'.format(file_name)
    }