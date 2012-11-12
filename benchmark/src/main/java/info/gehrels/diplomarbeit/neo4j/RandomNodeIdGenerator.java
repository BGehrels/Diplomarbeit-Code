package info.gehrels.diplomarbeit.neo4j;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;

public class RandomNodeIdGenerator implements Iterable<Integer> {
	private static int seed = 1;

	private final Random random = new Random(seed++);
	private final LinkedList<Integer> nodeIdPool = new LinkedList<>();
	private int remainingNumberOfResults;

	public RandomNodeIdGenerator(long maxNodeId, int maxNumberOfResults) {
		remainingNumberOfResults = maxNumberOfResults;
		for (int i = 0; i <= maxNodeId; i++) {
			nodeIdPool.add(i);
		}
	}

	@Override
	public Iterator<Integer> iterator() {
		return new Iterator<Integer>() {
			@Override
			public boolean hasNext() {
				return (remainingNumberOfResults > 0) && (nodeIdPool.size() > 0);
			}

			@Override
			public Integer next() {
				System.out.println(remainingNumberOfResults);
				remainingNumberOfResults--;
				return nodeIdPool.remove(random.nextInt(nodeIdPool.size()));
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};

	}
}
