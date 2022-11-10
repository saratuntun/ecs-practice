import pandas as pd, os
from pymongo import MongoClient

class mongo_ops:
    def __init__(self, db:str, host:str='127.0.0.1', port:int=27017, username=None, password=None, 
                 atlas:bool=False, **kwargs):
        """ A util for making a connection to mongo """
        if username and password:
            if atlas:
                # username : mlproduction
                # host: cluster0.xuddjop.mongodb.net
                mongo_uri = f"mongodb+srv://{username}:{password}@{host}/?retryWrites=true&w=majority"
            else:
                mongo_uri = 'mongodb://%s:%s@%s:%s' % (username, password, host, port)
            self.conn = MongoClient(mongo_uri)[db]
        else:
            client = MongoClient(host, port)
            self.conn = client[db]
            
    @property
    def collections_names(self):
        return self.conn.list_collection_names()
        
        
    def insert(self, collection:str, entry:dict):
        """
        param:
            entry:
                e.g. d = {'website': 'www.carrefax.com', 'author': 'Daniel Hoadley', 'colour': 'purple'}
        """
        # Create connection to MongoDB
        coll = self.conn[collection]
        coll.insert(d)
        
    def insert_many(self, collection:str, entries:list):
        """
        param:
            entry: list of dictionary (d)
                e.g. d = {'website': 'www.carrefax.com', 'author': 'Daniel Hoadley', 'colour': 'purple'}
        """
        # Create connection to MongoDB
        coll = self.conn[collection]
        coll.insert_many(entries)

    def read(self, collection:str, query:dict={}, no_id=False):
        """ Read from Mongo and Store into DataFrame """
        # Connect to MongoDB
        cursor = self.conn[collection].find(query)

        # Expand the cursor and construct the DataFrame
        df =  pd.DataFrame(list(cursor))

        # Delete the _id
        if no_id:  del df['_id']
        return df
    
    def trim_to_N(self, collection, limit=int(1e7)):
        coll = self.conn[collection]
        mycol.find().sort("timestamp")
    
    
    def import_df(self, collection:str, df:pd.DataFrame):
        """
        insert dataframe to mongo
        """
        self.conn[collection].insert_many(df.to_dict('records'))
        print("Added.")
        
    def drop(self, collection: str):
        self.conn[collection].drop()
        print("Dropped.")