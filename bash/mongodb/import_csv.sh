#mongo use movielog
export csvfile='movie-rateonly'
export collection_name='movie'
mongoimport --db=movielog --collection=$collection_name --type=csv --headerline --file=$csvfile --mode=insert