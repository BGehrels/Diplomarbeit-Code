package info.gehrels.diplomarbeit.flockdb;

import info.gehrels.diplomarbeit.AbstractStronglyConnectedComponentsCalculator;
import info.gehrels.diplomarbeit.IterableIterator;
import info.gehrels.diplomarbeit.Measurement;
import info.gehrels.flockDBClient.FlockDB;

import java.io.IOException;

import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.diplomarbeit.flockdb.FlockDBHelper.createFlockDB;
import static info.gehrels.diplomarbeit.flockdb.FlockDBHelper.getAllOutgoingRelationshipsFor;
import static java.lang.Long.parseLong;

public class FlockDBStronglyConnectedComponents extends AbstractStronglyConnectedComponentsCalculator<Long> {
	protected final FlockDB graphDB;
	private final long maxNodeId;

	public static void main(final String... args) throws Exception {
		measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				new FlockDBStronglyConnectedComponents(createFlockDB(), parseLong(args[0]))
					.calculateStronglyConnectedComponents();
			}
		});
	}

	public FlockDBStronglyConnectedComponents(FlockDB flockDB, long maxNodeId) throws IOException {
		this.graphDB = flockDB;
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
