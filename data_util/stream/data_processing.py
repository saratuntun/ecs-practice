from pathlib import Path
import requests, time
from datetime import datetime
import numpy as np, pandas as pd
#
from sklearn.preprocessing import MultiLabelBinarizer


class processor:
    def __init__(self, data_fp:str=None, ip='128.2.204.215', frac=None, chunksize=None, debug:bool=False):
        if data_fp is not None:
            data = pd.read_csv(data_fp, header=None, on_bad_lines='skip')
            if frac: data = data.sample(frac=frac)
            print(data.columns)
            data.columns = ['timestamp', 'user_id', 'get_request']
            self.chunksize = chunksize
            if chunksize:
                req_data = pd.concat([x.apply(self.clean_request) for x in np.split(data.get_request, np.arange(chunksize, len(data), chunksize))])
            else:
                req_data = data.get_request.apply(self.clean_request)
            data = pd.concat([data.drop('get_request', axis=1), req_data], axis=1)
            print(f"Size:{data.shape}, \nColumns:{data.columns}")
            # Cleaned data (field extractd)
            self.alt_data = data.copy()[['timestamp', 'user_id', 'request_type', 'movie_id', 'rating']]
        #
        self.req_ip = ip
        self.debug = debug
        self.cols_to_drop = [
            'get_request','belongs_to_collection', 'release_date', 'overview', 'homepage', 'budget', 'poster_path', 
            'id','tmdb_id','imdb_id', 'title','original_title'
        ]
        
    def main(self, expand_features: bool=False, save_path=None):
        chunksize = self.chunksize
        if self.debug: print("Adding time info")
        n = len(self.alt_data)
        if chunksize:
            self.time_data = pd.concat([x.apply(self.expand_time) for x in np.split(self.alt_data.timestamp, np.arange(chunksize, n, chunksize))])
        else:
            self.time_data = self.alt_data.timestamp.apply(self.expand_time)
        self.alt_data = pd.concat([self.time_data.astype(int), self.alt_data.drop('timestamp', axis=1)], axis=1)
        #
        if self.debug: print(self.alt_data.head(2))
        else: del self.time_data
        if expand_features:
            if self.debug: print("Adding user info")
            if chunksize:
                self.user_data = pd.concat([
                    x.apply(self.expand_user) for x in np.split(self.alt_data.user_id, np.arange(chunksize, n, chunksize))])
            else:
                self.user_data = self.alt_data.user_id.apply(self.expand_user)
            self.alt_data = pd.concat([self.alt_data, self.user_data.convert_dtypes()], axis=1)
            del self.user_data
            if self.debug: 
                print(self.alt_data.head(2))
                print("Adding movie info")
            if chunksize:
                self.movie_data = pd.concat([
                    x.apply(self.expand_movie) for x in np.split(self.alt_data.movie_id, np.arange(chunksize, n, chunksize))])
            else:
                self.movie_data = self.alt_data.movie_id.apply(self.expand_movie)
        # 
        self.alt_data = self.remove_excess_data(self.alt_data, self.cols_to_drop)
        self.alt_data = self.alt_data.loc[:, ~self.alt_data.columns.duplicated()]
        if save_path:
            self.alt_data.to_csv(save_path, header, index=None)
        return self.alt_data
    
    def clean_request(self, req):
        stuff = req.split('\\s+')[-1].split('/')
        try:
            if len(stuff)>4:  # rating
                req_type, movie_id, rating, time = stuff[1], stuff[3], pd.NA, stuff[-1].split('.')[0]
            else:
                req_type, (movie_id, rating), time = 'rate', stuff[2].split('='), pd.NA
        except Exception as e:
            print(f"Request: {stuff}")
                  
        return pd.Series({
            'request_type': req_type, 
            'movie_id': movie_id,
            'movie_time': time,
            'rating': rating
        })
        
    def expand_time(self, timestamp: pd.Series):
        timestamp = time.strptime(timestamp[:16],'%Y-%m-%dT%H:%M')
        info = {
            'year': lambda t: t.tm_year,
            'month': lambda t: t.tm_mon,
            'day': lambda t: t.tm_mday,
            'hour': lambda t: t.tm_hour,
            'weekday': lambda t: t.tm_wday,
            #'minute': lambda t: t.tm_min,
            #'second': lambda t: t.tm_sec,
        }
        time_df = pd.Series(index=info.keys(), dtype = 'int')
        for k, f in info.items():
            time_df[k] = f(timestamp)
        return time_df
    
    def expand_user(self, user_id, timeout=0.5):
        print(f"Requesting")
        res_data = requests.get(f'http://{self.req_ip}:8080/user/{user_id}', timeout=timeout).json()
        print(f"Requested")
        try:
            res_data['occupation'] = res_data['occupation'].split('/')[0]
        except Exception as e:
            if self.debug:print(f"Error for user: {user_id}: {res_data}")
        return pd.Series(res_data, dtype = 'string')
    
    def expand_movie(self, movie_id, timeout=0.5):
        res = requests.get(f'http://{self.req_ip}:8080/movie/{movie_id}', timeout=timeout)
        movie_info = res.json()
        # data filtering
        for k, v in movie_info.items():
            if type(v)==list and len(v)>=1:
                # ['genre', 'production_companies', 'production_countries']
                if 'id' in v[0]:
                    movie_info[k] = [g['id'] for g in v]
                elif 'name' in v[0]:
                    movie_info[k] = [g['name'] for g in v]
        if 'release_data' in movie_info:
            movie_info['release_year'] = int(movie_info['release_date'].split('-')[0])
        movie_info = pd.Series(movie_info)
        return movie_info.drop(self.cols_to_drop, errors='ignore')
        # 
    def remove_excess_data(self, data, cols_to_drop:list):
        data = data.drop(cols_to_drop, axis=1, errors='ignore')
        return data
    
def query_data(ID:int, category:str='user', ip:str='128.2.204.215'):
    if category=='user':
        res = requests.get(f'http://{ip}:8080/user/{ID}')
    elif category =='movie':
        res = requests.get(f'http://{ip}:8080/movie/{ID}')
    return res.json()