Aggregation Queries on top of log events
=======================================

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