package org.neo4j.experiments;

import static java.util.stream.Collectors.toList;
import static org.neo4j.driver.Values.parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.RepeatedTest;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.MapAccessor;
import org.neo4j.driver.types.Node;

public class SimpleCopyTest extends BaseTest {

	@RepeatedTest(REPEAT_COUNT)
	void copyAllNodes() {

		List<Node> nodesBuffer = new ArrayList<>(BATCH_SIZE);

		try (Session session = sourceDriver.session()) {
			session.run(READ_QUERY)
					.stream()
					.map(record -> record.get(0).asNode())
					.forEach(node -> {
						nodesBuffer.add(node);
						if (nodesBuffer.size() == BATCH_SIZE) {
							System.out.print('r');
							writeNodes(nodesBuffer);
							nodesBuffer.clear();
						}
					});
			writeNodes(nodesBuffer);
		}
	}

	private void writeNodes(Collection<Node> entries) {

		try (Session session = getTargetSession()) {
			List<Map<String, Object>> mapStream = entries.stream().map(MapAccessor::asMap).collect(toList());
			session.writeTransaction(w -> w.run(WRITE_QUERY, parameters("entries", mapStream))).consume();
		}
		System.out.print("W");
	}

}
