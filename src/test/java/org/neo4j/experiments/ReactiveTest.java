package org.neo4j.experiments;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.driver.Values.parameters;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Values;
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

		Integer createdCount = readNodes()
				.buffer(BATCH_SIZE)
				.doOnEach(it -> logBatchRead())
				.flatMap(this::writeNodes, WRITER_CONCURRENCY)
				.reduce(0, (count, result) -> count + result.counters().nodesCreated())
				.block();
		assertEquals(sourceNodesCount, createdCount);
	}

	private Flux<Node> readNodes() {
		return Flux.usingWhen(Mono.fromSupplier(getSourceRxSession()),
				session -> session.readTransaction(tx -> Flux.from(tx.run(READ_QUERY).records())
						.map(record -> record.get(0).asNode())
						.doOnSubscribe(it -> LOG.info("Start reading")))
				, RxSession::close)
			.doOnComplete(() -> LOG.info("\nReading complete"));
	}

	private Mono<ResultSummary> writeNodes(List<Node> nodes) {
		return Flux.usingWhen(Mono.fromSupplier(getTargetRxSession()),
				session -> session.writeTransaction(tx -> {
					Stream<Map<String, Object>> nodeData = nodes.stream()
							.map(MapAccessor::asMap);
					return tx.run(WRITE_QUERY, parameters("entries", Values.value(nodeData))).consume();
				})
				, RxSession::close)
				.doOnNext(it -> logBatchWrite())
				.single();
	}
}
