CREATE KEYSPACE reservation_system WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2};
USE reservation_system;

CREATE TABLE users (
    id uuid,
    username varchar,
    creation timestamp,
    modification timestamp,
    PRIMARY KEY (id)
);

CREATE INDEX ON users (username);

CREATE TABLE movies (
    name varchar,
    date timestamp,
    creation timestamp,
    modification timestamp,
    PRIMARY KEY ( name )
);

CREATE TABLE reservations (
    movie_name varchar,
    user_id uuid,
    seat varchar,
    creation timestamp,
    modification timestamp,
    PRIMARY KEY (movie_name, seat)
);