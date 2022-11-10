import pytest
from data_util.test import *
from pathlib import Path
import json, yaml, argparse
from types import SimpleNamespace

conf = yaml.safe_load(Path(Path('config/test.yaml')).read_text())
conf = json.loads(json.dumps(conf, indent=4), object_hook=lambda item: SimpleNamespace(**item))
conf.DATA.STREAM.stream_data_path = Path(conf.DATA.data_path)/(conf.DATA.STREAM.stream_name+'.csv')
conf.DATA.STREAM.processed_data_path = Path(conf.DATA.data_path)/(conf.DATA.STREAM.stream_name+conf.DATA.STREAM.processed_suffix+'.csv')

def test_main():
    test_kafka_consumer(conf.DATA.STREAM.topic)
    test_kafka_stream_csv(conf)
    test_processor(conf.DATA.STREAM.stream_data_path)
    test_UpdateData(conf)

if __name__=="__main__":
    

    ap = argparse.ArgumentParser()
    ap.add_argument('--data_config','-data_config',  default='config/test.yaml', type=str,  help='path to data config file (yaml) is')
    args = ap.parse_args()
    #
    conf = yaml.safe_load(Path(args.data_config).read_text())
    conf = json.loads(json.dumps(conf, indent=4), object_hook=lambda item: SimpleNamespace(**item))
    #
    conf.DATA.STREAM.stream_data_path = Path(conf.DATA.data_path)/(conf.DATA.STREAM.stream_name+'.csv')
    conf.DATA.STREAM.processed_data_path = Path(conf.DATA.data_path)/(conf.DATA.STREAM.stream_name+conf.DATA.STREAM.processed_suffix+'.csv')
    print(f"Stream configurations: {vars(conf)}")
    test_main()
    

    
    
    