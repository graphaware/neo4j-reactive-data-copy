
- run the databases (for example using the `docker-compose` file)
- Create some test data using

```
CREATE OR REPLACE DATABASE mytestdb;
:use mytestdb;
CREATE INDEX TestIdIdx IF NOT EXISTS FOR (t:Test) ON (t.id);
CALL apoc.periodic.iterate("UNWIND range(1, 1000000) as i RETURN i", 
"CREATE (n:Test{id:'node id ' + i, data: apoc.text.random(1000)})", {batchSize:10000});
```

- `mvn test`