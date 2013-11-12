Queries
=======

MongoDB - Aggregation Queries on top of log events
--------------------------------------------------

1. Get number of times a status code has appeared

```
use logs
db.logEvents.aggregate([
    {$group:
        {
            _id: "$response_code",
            status_count:{$sum:1}
        }
    }
])
```

2. Co-relates request page to response code

```
db.logEvents.aggregate([
    {$group:
        {
            _id: { "response": "$response_code",  "access_page": "$request_page"},
            status_count:{$sum:1}
        }
    },
    {$sort:
        {
            "_id.access_page": 1,
            "_id.response": 1
        }
    }
])
```

3. Count total number of bytes served for each page

```
db.logsOld.aggregate([
    {$group:
        {
            _id: { "access_page": "$request_page" },
            size:{$sum:"$response_bytes"}
        }
    }
])
```

4. How many times a client visited a particular page

```
db.logEvents.aggregate([
    {$group:
        {
            _id: "$ip",
            size:{$sum:1}
        }
    },
    {$project:
        {
            _id: 0,
            requester: "$_id",
            visit_count: "$size"
        }
    }
])
```

5. Top 10 visitors

```
db.logEvents.aggregate([
    {$group:
        {
            _id: "$ip",
            size:{$sum:1}
        }
    },
    {$project:
        {
            _id: 0,
            requester: "$_id",
            visit_count: "$size"
        }
    },
    {$sort:
        {
            visit_count: -1
        }
    },
    {$limit: 10}
])
```

6. Top Browsers

```
db.logEvents.aggregate([
    {$group:
        {
            _id: {
                "browser": "$user_agent"
            },
            status_count: {$sum:1}
        }
    }
])
```

Cassandra - Queries
-------------------
1. customer movie watch history

```
SELECT cid,movie_name,ts FROM moviedata.watch_history WHERE cid=<customer_id>;
```

2. Create secondary index on watch histories 'timestamp' column

```
CREATE INDEX ON moviedata.watch_history(ts);
```

3. Check which customers watched a movie with in a specific time period

```
SELECT cid, movie_name FROM moviedata.watch_history WHERE ts > <begin_timestamp> AND ts < <end_timestamp>;
```

4. Check all the movie ratings given by specific user

```
SELECT customer_name, movie_name, rating FROM moviedata.customer_rating WHERE cid=<customer_id>;
```

5. Check all the ratings and which user given to a specific movie

```
SELECT customer_name, movie_name, rating FROM moviedata.customer_rating WHERE movie_name=<value> ALLOW FILTERING;
```

> Error: Bad Request: No indexed columns present in by-columns clause with Equal operator

6. Get all the movies of specific genre:

```
SELECT * FROM moviedata.movies_genre WHERE genre=<value>;
```

7. Get movies released in specific year:

```
CREATE INDEX ON moviedata.movies_genre(movie_name);
SELECT * FROM moviedata.movies_genre WHERE release_year=<year> ALLOW FILTERING;
```

8. Get the genre, release_date of a movie:

```
SELECT genre, release_year FROM moviedata.movies_genre WHERE movie_name=<value>;
```

9. List the wish list of a customer

```
SELECT * FROM moviedata.customer_queue WHERE cid=<customer_id>;
```
