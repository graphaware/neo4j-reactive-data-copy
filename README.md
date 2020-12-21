
This is the source code for my experiments on using Neo4j in a reactive way.

Check out the GraphAware blog for more details. 

### Running the tests

- Start the Neo4j instances (for example using the `docker-compose` file, but on MacOS I advise to run them without docker)
  
- Make sure the test configuration and database ports are in sync 
  (the tests use bolt on port 8687 for the source database, 9687 for the target database)
  
- Create some test data in the source database using

```
:use system
```
```
CREATE OR REPLACE DATABASE mytestdb;
```
```
:use mytestdb
```
```
CREATE INDEX TestIdIdx IF NOT EXISTS FOR (t:Test) ON (t.id);
CALL apoc.periodic.iterate("UNWIND range(1, 1000000) as i RETURN i", 
  "CREATE (n:Test{id:'node id ' + i, data: apoc.text.random(1000)})", {batchSize:10000});
```

- Run the tests from the command line using `mvn test`