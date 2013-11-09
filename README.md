Mongo Example:

Inserting data:

```
bin/run mongo --mode insert 100000 1000000 100000000
```

Inserting data with indexing:

```
bin/run mongo --mode insert 100000 1000000 100000000 --indexData
```

Inserting data with indexing and custom batch size:

```
bin/run mongo --mode insert 100000 1000000 100000000 --indexData --batchSize 5000
```