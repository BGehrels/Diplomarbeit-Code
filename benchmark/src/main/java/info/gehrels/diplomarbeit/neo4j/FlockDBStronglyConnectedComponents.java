package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import info.gehrels.flockDBClient.FlockDB;

import java.io.IOException;
import java.util.Iterator;
import static info.gehrels.diplomarbeit.neo4j.FlockDBHelper.getAllOutgoingRelationshipsFor;

public class FlockDBStronglyConnectedComponents extends AbstractStronglyConnectedComponentsCalculator<FlockDB, Long> {
	private final long maxNodeId;

	public static void main(String... args) throws Exception {
		Stopwatch stopwatch = new Stopwatch().start();
		new FlockDBStronglyConnectedComponents(Long.parseLong(args[0])).calculateStronglyConnectedComponents();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	public FlockDBStronglyConnectedComponents(long maxNodeId) throws IOException {
		super(FlockDBHelper.createFlockDB());

		this.maxNodeId = maxNodeId;
	}

	@Override
	protected Iterable<Long> getAllNodes() {
		return new Iterable<Long>() {
			@Override
			public Iterator<Long> iterator() {
				return new Iterator<Long>() {
					public long nextId;

					@Override
					public boolean hasNext() {
						return nextId <= maxNodeId;
					}

					@Override
					public Long next() {
						return hasNext() ? nextId++ : null;
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}
		};
	}

	@Override
	protected long getNodeName(Long node) {
		return node;
	}

	@Override
	protected Iterable<Long> getOutgoingIncidentNodes(Long node) throws Exception {
		return getAllOutgoingRelationshipsFor(graphDB, node);
	}
}
