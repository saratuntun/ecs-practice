use movielog;
db['stream-rateonly'].aggregate(
    [
        {
            $lookup: {
                from: 'movie-rateonly_nodub',
                localField: 'movie_id',
                foreignField: 'movie_id',
                as: 'movie_info'
            }
        },
        {
            $lookup: {
                from: 'user-rateonly_nodub',
                localField: 'user_id',
                foreignField: 'user_id',
                as: 'user_info'
            }
        },
        { $out : "combined-rateonly" }
    ]
);