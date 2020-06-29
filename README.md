
- run the databases (for example using the `docker-compose` file)
- Create some test data using

```
call apoc.periodic.iterate("UNWIND range(1, 1000000) as i RETURN i", "CREATE (n:Test{id:'node id ' + i, data: i + 'test data test data test data ' + i})", {batchSize:10000})
yield batches, total return batches, total
```

- `mvn test`