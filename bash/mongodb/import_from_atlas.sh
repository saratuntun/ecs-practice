export collection="combined-rateonly"
export db="movielog"
#
export uri="mongodb://127.0.0.1:27017/$db"  
# local 
mongoexport --uri $uri --collection $collection --out "./$collection" 
# from atlas
#export uri="mongodb+srv://cluster0.xuddjop.mongodb.net/$db"  
#mongoexport --uri $uri --collection $collection --out "./$collection" --username mlproduction --password pulp_prediction