# BDDS Project: Distributed System for cinema reservation.
The project is done in python using cassandra-driver library. The cli provides ability to create users, create movies, register/update seats for movies, view all movies/seats/users and other utilities.
There are also 3 stress tests:

- Stress Test 1: The client makes the same request very quickly.
- Stress Test 2: Two or more clients make the possible requests randomly.
- Stress Test 3: Immediate occupancy of all seats/reservations on 2 clients.

# IMPORTANT
- Please use Python 3.10.11 (newer versions of python have changes in cassandra-driver library and won't work)

# Usage
- 1. start containers: <br>
  `docker-compose up`
- 2. after all 3 containers are healthy, start application:
  `python cli.py`, all needed functionalities are available after running this script.
- 3. when done using application close containers:
  `docker-compose down`

# Database schema
```sql
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
```
