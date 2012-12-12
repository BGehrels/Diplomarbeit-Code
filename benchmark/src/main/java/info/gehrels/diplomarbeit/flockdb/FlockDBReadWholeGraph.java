package info.gehrels.diplomarbeit.flockdb;

import info.gehrels.diplomarbeit.AbstractReadWholeGraph;
import info.gehrels.diplomarbeit.Measurement;
import info.gehrels.flockDBClient.FlockDB;
import info.gehrels.flockDBClient.PagedNodeIdList;
import info.gehrels.flockDBClient.SelectionQuery;

import java.io.IOException;
import java.util.List;

import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.flockDBClient.Direction.OUTGOING;

public class FlockDBReadWholeGraph extends AbstractReadWholeGraph {
	private FlockDB flockDB;
	private final long maxNodeId;

	public static void main(final String... args) throws Exception {
		measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				new FlockDBReadWholeGraph(new FlockDB("localhost", 7915, 1000000), Long.parseLong(args[0]), true)
					.readWholeGraph();
			}
		});
	}

	public FlockDBReadWholeGraph(FlockDB db, long maxNodeId, boolean writeToStdOut) throws IOException {
		super(writeToStdOut);
		this.flockDB = db;
		this.maxNodeId = maxNodeId;
	}

	// TODO: Statt dessen vielleicht UNION Query? oder mehrere Parallele?
	public void readWholeGraph() throws Exception {
		for (long nodeId = 0; nodeId <= maxNodeId; nodeId++) {
			for (byte graphId = 1; graphId <= 4; graphId++) {
				readAllEdgesForNode(graphId, nodeId);
			}
		}
	}

	private void readAllEdgesForNode(byte graphId, long nodeId) throws Exception {
		List<PagedNodeIdList> result
			= flockDB
			.select(SelectionQuery.simpleSelection(nodeId, graphId, OUTGOING))
			.execute();
		for (long singleQueryResult : new NonPagedResultList(result.get(0))) {
			write(
				nodeId,
			    "L" + graphId,
			    singleQueryResult
			);
		}
	}

}
