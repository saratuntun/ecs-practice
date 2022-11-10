from data_util.updateStreamData import UpdateData

import json, yaml, argparse
from types import SimpleNamespace
from pathlib import Path

if __name__=="__main__":
    import json, yaml, argparse
    from types import SimpleNamespace

    ap = argparse.ArgumentParser()
    ap.add_argument('--data_config','-data_config',  default='config/data.yaml', type=str,  help='path to data config file (yaml) is')
    args = ap.parse_args()
    #
    conf = yaml.safe_load(Path(args.data_config).read_text())
    conf = json.loads(json.dumps(conf, indent=4), object_hook=lambda item: SimpleNamespace(**item))
    #
    conf.DATA.STREAM.stream_data_path = Path(conf.DATA.data_path)/(conf.DATA.STREAM.stream_name+'.csv')
    conf.DATA.STREAM.processed_data_path = Path(conf.DATA.data_path)/(conf.DATA.STREAM.stream_name+conf.DATA.STREAM.processed_suffix+'.csv')
    print(f"Stream configurations: {vars(conf)}")
    UpdateData(conf)
    