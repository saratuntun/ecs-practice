export db=movielog
export coll=stream-rateonly
export fields=timestamp,user_id,request_type,movie_id,rating
mongoexport --db=$db --collection=$coll --type=csv --fields=$fields --noHeaderLine --out=stream_rateonly.csv 