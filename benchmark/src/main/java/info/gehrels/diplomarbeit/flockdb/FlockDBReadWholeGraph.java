package info.gehrels.diplomarbeit.flockdb;

import com.google.common.base.Stopwatch;
import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.flockDBClient.FlockDB;
import info.gehrels.flockDBClient.PagedNodeIdList;
import info.gehrels.flockDBClient.SelectionQuery;

import java.io.IOException;
import java.util.List;

import static info.gehrels.flockDBClient.Direction.OUTGOING;

public class FlockDBReadWholeGraph {
	private FlockDB flockDB;
	private final long maxNodeId;

	int numberOfResults = 0;

	public FlockDBReadWholeGraph(FlockDB db, long maxNodeId) throws IOException {
		this.maxNodeId = maxNodeId;
		flockDB = db;
	}

	public static void main(String... args) throws IOException, FlockException {
		Stopwatch stopwatch = new Stopwatch().start();
		new FlockDBReadWholeGraph(new FlockDB("localhost", 7915, 1000000), Long.parseLong(args[0])).readWholeGraph(true).shutdown();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	private FlockDBReadWholeGraph shutdown() {
		return this;
	}

	// TODO: Statt dessen vielleicht UNION Query? oder mehrere Parallele?
	public FlockDBReadWholeGraph readWholeGraph(boolean writeToStdOut) throws IOException, FlockException {
		for (long nodeId = 0; nodeId <= maxNodeId; nodeId++) {
			for (byte graphId = 1; graphId <= 15; graphId++) {
				readAllEdgesForNode(graphId, nodeId, writeToStdOut);
			}
		}
		return this;
	}

	private void readAllEdgesForNode(byte graphId, long nodeId, boolean writeToStdOut) throws
		IOException, FlockException {
		List<PagedNodeIdList> result
			= flockDB
			.select(SelectionQuery.simpleSelection(nodeId, graphId, OUTGOING))
			.execute();
		for (long singleQueryResult : new NonPagedResultList(result.get(0))) {
			String output = nodeId + ", L" + graphId + ", " + singleQueryResult;
			if (writeToStdOut) {
				System.out.println(output);
			}
		}
	}

}
