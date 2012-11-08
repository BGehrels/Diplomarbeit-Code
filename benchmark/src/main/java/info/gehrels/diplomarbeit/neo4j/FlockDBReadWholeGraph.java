package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.flockDBClient.FlockDB;
import info.gehrels.flockDBClient.PagedNodeIdList;
import info.gehrels.flockDBClient.SelectionQuery;

import java.io.IOException;
import java.util.List;

public class FlockDBReadWholeGraph {
	private FlockDB flockDB;
	private final long maxNodeId;

	public FlockDBReadWholeGraph(long maxNodeId) throws IOException {
		this.maxNodeId = maxNodeId;
		flockDB = new FlockDB("localhost", 7915, 1000000);
	}

	public static void main(String... args) throws IOException, FlockException {
		Stopwatch stopwatch = new Stopwatch().start();
		new FlockDBReadWholeGraph(Long.parseLong(args[0])).readWholeGraph().shutdown();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	private FlockDBReadWholeGraph shutdown() {
		return this;
	}

	private FlockDBReadWholeGraph readWholeGraph() throws IOException, FlockException {
		for (byte graphId = 1; graphId <= 15; graphId++) {
			for (long nodeId = 0; nodeId <= maxNodeId; nodeId++) {
				readAllEdgesForNode(graphId, nodeId);
			}
		}
		return this;
	}

	private void readAllEdgesForNode(byte graphId, long nodeId) throws IOException, FlockException {
		List<PagedNodeIdList> result
			= flockDB
			.select(SelectionQuery.simpleSelection(nodeId, graphId, false))
			.execute();
		for (PagedNodeIdList singleQueryResult : result) {
			readWholeNodeIdList(graphId, nodeId, singleQueryResult);
		}
	}

	private void readWholeNodeIdList(byte graphId, long nodeId, PagedNodeIdList singleQueryResult) throws IOException,
		FlockException {
		boolean first = true;
		do {
			if (first) {
				first = false;
			} else {
				singleQueryResult = singleQueryResult.getNextPage();
			}

			for (Long destNodeId : singleQueryResult) {
				long blubb = graphId + nodeId + destNodeId;
			}
		} while (singleQueryResult.hasNextPage());
	}
}
