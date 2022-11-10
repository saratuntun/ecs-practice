use movielog;
db['user-rateonly'].aggregate(
    [ 
        { "$sort": { "_id": 1 } }, 
        { "$group": { 
            "_id": "$user_id", 
            "doc": { "$first": "$$ROOT" } 
        }}, 
        { "$replaceRoot": { "newRoot": "$doc" } },
        { "$out": "user-rateonly_nodup" }
    ]
);
db['movie-rateonly'].aggregate(
    [ 
        { "$sort": { "_id": 1 } }, 
        { "$group": { 
            "_id": "$movie_id", 
            "doc": { "$first": "$$ROOT" } 
        }}, 
        { "$replaceRoot": { "newRoot": "$doc" } },
        { "$out": "movie-rateonly_nodup" }
    ]
);