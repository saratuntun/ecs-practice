from .stream.data_processing import processor, query_data
from .stream.data_stream import kafka_streamer
from .mongo.mongo_query import mongo_ops
#
import pandas as pd, os, gc
#
import json, yaml, argparse
from types import SimpleNamespace
from pathlib import Path


def mkdirifNe(path):
    if not Path(path).exists(): Path(path).mkdir(parents=True, exist_ok=True)

def UpdateData(conf):
    "Update data stream from fetch new data, processing and adding to current datafile"
    mkdirifNe(conf.DATA.data_path)
    # check if file previously exist
    if conf.DATA.STREAM.stream_data_path.exists():
        existing = True
        conf.DATA.STREAM.stream_data_path_temp = Path(conf.DATA.data_path)/'_temp_stream.csv'
        stream_path = conf.DATA.STREAM.stream_data_path_temp
    else:
        existing = False
        stream_path = conf.DATA.STREAM.stream_data_path
    # Stream
    streamer = kafka_streamer(topic=conf.DATA.STREAM.topic)
    streamer.stream_to_csv_ratings(conf.DATA.STREAM.num_new_messages_per_update, stream_path)
        # merge if needed
    if existing:
        existing_stream_df = pd.read_csv(conf.DATA.STREAM.stream_data_path, header=None).head(2)
        stream_df = pd.read_csv(stream_path, header=None)
        # processed_df
        try:
            violate = False
            existing_feat_df = pd.read_csv(conf.DATA.STREAM.processed_data_path, header=None).head(2)
        except:
            print("Violation")
            violate = True
            # Test mismatch of shape ./. old & new data
        if len(existing_stream_df.columns) != len(stream_df.columns) or violate:    
            print("[WARNING] Existing data shape does not match newly streamed. Old stream data will be removed")
            os.remove(conf.DATA.STREAM.stream_data_path)
            os.remove(conf.DATA.STREAM.processed_data_path)
            os.rename(stream_path, conf.DATA.STREAM.stream_data_path)
        else:
            os.remove(stream_path)
            pd.concat([existing_stream_df, stream_df]).to_csv(conf.DATA.STREAM.stream_data_path, header=None, index=None)
        del existing_stream_df, stream_df
        gc.collect()
        # keep max size
    if conf.DATA.STREAM.max_ent:  
        stream_df = pd.read_csv(conf.DATA.STREAM.stream_data_path, header=None)
        stream_df.iloc[max(0, int(stream_df.shape[0]-conf.DATA.STREAM.max_ent)):].to_csv(conf.DATA.STREAM.stream_data_path, header=None, index=None)
    # PROCESS data
    preprocessor = processor(conf.DATA.STREAM.stream_data_path, frac=None, chunksize=5000, debug=False)
    if conf.DATA.STREAM.add_external_features:
        processed_df =  preprocessor.main()
    else:
        processed_df = preprocessor.alt_data
    processed_df.to_csv(conf.DATA.STREAM.processed_data_path, mode='a', header=False)
    
    # Put to mongo
    if conf.MONGO_ATLAS.upload:
        try:
            atlas_mongo = mongo_ops(**conf.MONGO_ATLAS.CONNECTION.__dict__, atlas=True)
            atlas_mongo.import_df(conf.MONGO_ATLAS.CONNECTION.insert_to_collection, processed_df)
        except Exception as e:
            print(f"Upload to atlas error: {e}")

if __name__=="__main__":

    ap = argparse.ArgumentParser()
    ap.add_argument('--data_config','-data_config',  default='./config/data.yaml', type=str,  help='path to data config file (yaml) is')
    args = ap.parse_args()
    #
    conf = yaml.safe_load(Path(args.data_config).read_text())
    conf = json.loads(json.dumps(conf, indent=4), object_hook=lambda item: SimpleNamespace(**item))
    #
    conf.DATA.STREAM.stream_data_path = Path(conf.DATA.data_path)/(conf.DATA.STREAM.stream_name+'.csv')
    conf.DATA.STREAM.processed_data_path = Path(conf.DATA.data_path)/(conf.DATA.STREAM.stream_name+conf.DATA.STREAM.processed_suffix+'.csv')
    print(f"Stream configurations: {vars(conf)}")
    UpdateData(conf)
    