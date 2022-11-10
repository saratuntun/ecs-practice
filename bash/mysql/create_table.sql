CREATE TABLE moviestream (
    id INT NOT NULL AUTO_INCREMENT,
    movie_id VARCHAR(255) NOT NULL ,
    title VARCHAR(255) NOT NULL,
    release_date INT NOT NULL,
    duration INT NOT NULL,
    PRIMARY KEY (id)
);