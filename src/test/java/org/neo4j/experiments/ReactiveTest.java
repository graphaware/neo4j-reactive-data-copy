package org.neo4j.experiments;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.types.MapAccessor;
import org.neo4j.driver.types.Node;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReactiveTest extends BaseTest {

	@RepeatedTest(REPEAT_COUNT)
	void copyAllNodes() {

		Integer createdCount = readNodes(READ_QUERY, Values.parameters())
				.doOnRequest((r) -> {if (r>1000) System.out.print("R("+r+")");})
				.buffer(BATCH_SIZE)
				.doOnEach(it -> System.out.print("r"))
//				.parallel(WRITER_THREAD_COUNT)
//				.runOn(Schedulers.boundedElastic())
				.flatMap(this::writeNodes)
//				.sequential()
				.reduce(0, (count, result) -> count + result.counters().nodesCreated())
				.block();
		assertEquals(sourceNodesCount, createdCount);
	}

	private Flux<Node> readNodes(String query, Value parameters) {
		return Flux.usingWhen(Mono.fromSupplier(sourceDriver::rxSession),
				session -> session.readTransaction(tx -> {
					RxResult result = tx.run(query, parameters);
					return Flux.from(result.records())
							.limitRate(30) // we need to limit the fetching rate or the driver is getting to much data too eagerly
							.flatMap(record -> Mono.just(record.get(0).asNode()));
				})
				, RxSession::close)
			.doOnComplete(() -> LOG.info("\nReading complete"));
	}

	private Flux<ResultSummary> writeNodes(List<Node> parameters) {
		String query = "UNWIND $entries as entry CREATE (n:" + TEST_LABEL + ") SET n = entry";
		return Flux.usingWhen(Mono.fromSupplier(getTargetRxSession()),
				session -> session.writeTransaction(tx -> {
					List<Map<String, Object>> nodeData = parameters.stream()
							.map(MapAccessor::asMap)
							.collect(toList());
					return tx.run(query, Values.parameters("entries", Values.value(nodeData))).consume();
				})
				, RxSession::close)
				.doOnNext(it -> System.out.print("W"));
	}
}
