import unittest, yaml, json, os
from .stream.data_stream import *
from .stream.data_processing import *
from .updateStreamData import UpdateData

def test_kafka_consumer(topic):
    stream = kafka_streamer(topic=topic)
    
    topics = stream.consumer.topics()
    # check consumer
    if not topics: 
        raise RuntimeError()    
    assert topic in topics, f"No requested stream present, only the following: {topics}"
    #
def test_kafka_stream_csv(conf):
    if Path(conf.DATA.STREAM.stream_data_path).exists(): os.remove(conf.DATA.STREAM.stream_data_path)
    streamer = kafka_streamer(topic=conf.DATA.STREAM.topic)
    streamer.stream_to_csv(conf.DATA.STREAM.num_new_messages_per_update, conf.DATA.STREAM.stream_data_path)
    assert len(pd.read_csv(conf.DATA.STREAM.stream_data_path, header=None))==conf.DATA.STREAM.num_new_messages_per_update
    os.remove(conf.DATA.STREAM.stream_data_path)
    #    
    streamer.stream_to_csv_ratings(conf.DATA.STREAM.num_new_messages_per_update, conf.DATA.STREAM.stream_data_path)
    assert len(pd.read_csv(conf.DATA.STREAM.stream_data_path, header=None))==conf.DATA.STREAM.num_new_messages_per_update
    streamer.close()
        
def test_processor(test_csv_path):
    # maintain same size
    df = pd.read_csv(test_csv_path, header=None)
    df.iloc[max(0, df.shape[0]-10):,:].to_csv(test_csv_path, header=False, index=False)
    # check processor
    preprocessor = processor(test_csv_path, frac=None, chunksize=None, debug=True)
    processed_df =  preprocessor.main(expand_features=False)
    nan_sum = processed_df.isnull().sum()
    # Should return a string
    assert nan_sum.sum()==0, f"NaN exist for columns:{nan_sum}"
    assert len(processed_df) == len(df)
    
def test_UpdateData(conf):
    UpdateData(conf)
    
if __name__ == '__main__':
    
    ap = argparse.ArgumentParser()
    args = ap.parse_args()
    