package org.neo4j.experiments;

import static java.util.stream.Collectors.toList;
import static org.neo4j.driver.Values.parameters;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Session;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.types.MapAccessor;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BlockingQueueTest extends BaseTest {

	private static final Logger LOG = LoggerFactory.getLogger(BlockingQueueTest.class);

	private final BlockingQueue<Node> queue = new LinkedBlockingQueue<>(BATCH_SIZE * 2);
	private final Node poisonPill = new InternalNode(-1);
	private final AtomicInteger readCount = new AtomicInteger();

	@RepeatedTest(REPEAT_COUNT)
	void copyAllNodes() throws InterruptedException {

		queue.clear();
		List<Thread> threads = IntStream.range(0, WRITER_CONCURRENCY)
				.mapToObj(i -> new Thread(new EntryWriter(queue), "Writer thread " + i))
				.peek(Thread::start)
				.collect(Collectors.toList());

		readNodesAndEnqueue();

		for (Thread thread : threads) {
			thread.join();
		}
	}

	private void readNodesAndEnqueue() {
		try (Session session = getSourceSession()) {
			session.run(READ_QUERY)
					.stream()
					.map(record -> record.get(0).asNode())
					.forEach(this::enqueue);
		}
		queue.offer(poisonPill);
	}

	private void enqueue(Node node) {
		var count = readCount.incrementAndGet();
		if (count % BATCH_SIZE == 0)
			logBatchRead();
		try {
			queue.put(node);
		} catch (InterruptedException ignore) {
		}
	}

	private class EntryWriter implements Runnable {

		private final BlockingQueue<Node> queue;
		private int writeCount;

		public EntryWriter(BlockingQueue<Node> queue) {
			this.queue = queue;
		}

		@Override
		public void run() {
			boolean poisonPillReceived = false;
			Collection<Node> nodesToWrite = new HashSet<>(BATCH_SIZE);
			while (!poisonPillReceived) {
				nodesToWrite.clear();
				queue.drainTo(nodesToWrite, BATCH_SIZE);
				if (nodesToWrite.contains(poisonPill)) {
					poisonPillReceived = true;
					nodesToWrite.remove(poisonPill);
					queue.add(poisonPill);
				}
				if (!nodesToWrite.isEmpty()) {
					writeNodes(nodesToWrite);
					writeCount += nodesToWrite.size();
				}
			}
			LOG.info("Stopping writes - written {} entries", writeCount);
		}

		private void writeNodes(Collection<Node> entries) {

			try (Session session = getTargetSession()) {
				List<Map<String, Object>> mapStream = entries.stream().map(MapAccessor::asMap).collect(toList());
				session.writeTransaction(w -> w.run(WRITE_QUERY, parameters("entries", mapStream))).consume();
			}
			logBatchWrite();
		}
	}
}
