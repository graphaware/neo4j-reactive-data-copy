package org.neo4j.experiments;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.reactive.RxSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BaseTest {

	protected static final int REPEAT_COUNT = 3;
	protected static final String TEST_LABEL = "Test";
	protected static final Logger LOG = LoggerFactory.getLogger(BaseTest.class);

	protected static final int WRITER_THREAD_COUNT = 4;

	protected static final String READ_QUERY = "MATCH (n) RETURN n";

	private static final AuthToken AUTH_TOKEN = AuthTokens.basic("neo4j", "pass");
	private static final Config DRIVER_CONFIG = Config.builder()
//			.withMaxConnectionPoolSize(WRITER_THREAD_COUNT + 1)
//			.withEventLoopThreads(WRITER_THREAD_COUNT + 1)
//			.withFetchSize(50)
			.build();
	private static final String TARGET_DB_NAME = "myTestDb";
	protected Driver sourceDriver;
	protected Driver targetDriver;

	protected static final int BATCH_SIZE = 1000;
	protected int sourceNodesCount;
	private long startTime;

	@BeforeAll
	void setUp() {
		sourceDriver = GraphDatabase.driver("bolt://localhost:8687", AUTH_TOKEN, DRIVER_CONFIG);
		targetDriver = GraphDatabase.driver("bolt://localhost:9687", AUTH_TOKEN, DRIVER_CONFIG);
	}

	@AfterAll
	void afterAll() {
		sourceDriver.closeAsync();
		targetDriver.closeAsync();
	}

	@BeforeEach
	void cleanupDb() {
		LOG.info("Starting {}", this.getClass().getSimpleName());
		LOG.info("Cleaning up target DB");
		targetDriver.session(SessionConfig.forDatabase("system"))
				.run("CREATE OR REPLACE DATABASE " + TARGET_DB_NAME).consume();
		LOG.info("Created new database: {}", TARGET_DB_NAME);
		getTargetSession().run("CREATE CONSTRAINT ON (t:" + TEST_LABEL + ") ASSERT t.id IS UNIQUE").consume();
		sourceNodesCount = sourceDriver.session().run("MATCH (n) RETURN count(n) as cnt").single().get("cnt").asInt();
		startTime = System.currentTimeMillis();
	}

	@AfterEach
	void checkCount() {
		LOG.info("Completed {} in {} ms", this.getClass().getSimpleName(), System.currentTimeMillis() - startTime);
		int targetNodesCount = getTargetSession().run("MATCH (n) RETURN count(n) as cnt").single().get("cnt").asInt();
		assertEquals(sourceNodesCount, targetNodesCount);
	}

	protected Session getTargetSession() {
		return targetDriver.session(SessionConfig.forDatabase(TARGET_DB_NAME));
	}

	protected Supplier<RxSession> getTargetRxSession() {
		return () -> targetDriver.rxSession(SessionConfig.forDatabase(TARGET_DB_NAME));
	}
}
