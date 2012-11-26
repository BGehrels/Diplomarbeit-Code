package info.gehrels.diplomarbeit.flockdb;

import com.google.common.base.Stopwatch;
import info.gehrels.diplomarbeit.AbstractStronglyConnectedComponentsCalculator;
import info.gehrels.diplomarbeit.IterableIterator;
import info.gehrels.flockDBClient.FlockDB;

import java.io.IOException;

import static info.gehrels.diplomarbeit.flockdb.FlockDBHelper.getAllOutgoingRelationshipsFor;

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
		return new IterableIterator<Long>() {
			public long nextId;

			@Override
			public boolean hasNext() {
				return nextId <= maxNodeId;
			}

			@Override
			public Long next() {
				return hasNext() ? nextId++ : null;
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
