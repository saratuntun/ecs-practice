#!/bin/bash
export collectione="combined-rateonly"
export jsonfile=$collection_name
# local
#mongoimport --db=movielog --collection=$collectione --type=json --file=$jsonfile --mode=insert
# to atlas
mongoimport --uri "mongodb+srv://mlproduction:pulp_prediction@cluster0.xuddjop.mongodb.net" --db movielog --collection=$collection --type=json --file=$jsonfile --drop 