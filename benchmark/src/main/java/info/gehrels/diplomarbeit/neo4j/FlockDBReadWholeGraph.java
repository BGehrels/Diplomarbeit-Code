package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.flockDBClient.FlockDB;
import info.gehrels.flockDBClient.PagedNodeIdList;
import info.gehrels.flockDBClient.SelectionQuery;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class FlockDBReadWholeGraph {
	private FlockDB flockDB;
	private final long maxNodeId;

	int numberOfResults = 0;

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
		for (long nodeId = 0; nodeId <= maxNodeId; nodeId++) {
			SortedSet<Triplet<Long,Long,Byte>> results = new TreeSet<>();
			for (byte graphId = 1; graphId <= 15; graphId++) {
				readAllEdgesForNode(graphId, nodeId, results);
			}

			printOutResults(results);
		}
		System.out.println(numberOfResults);
		return this;
	}

	private void printOutResults(SortedSet<Triplet<Long,Long,Byte>> results) {
		for (Triplet result : results) {
			System.out.println(result.elem1 + ", L" + result.elem3 + ", " + result.elem2);
			numberOfResults++;
		}
	}

	private void readAllEdgesForNode(byte graphId, long nodeId, SortedSet<Triplet<Long,Long,Byte>> results) throws IOException, FlockException {
		List<PagedNodeIdList> result
			= flockDB
			.select(SelectionQuery.simpleSelection(nodeId, graphId, true))
			.execute();
		for (PagedNodeIdList singleQueryResult : result) {
			readWholeNodeIdList(graphId, nodeId, singleQueryResult, results);
		}
	}

	private void readWholeNodeIdList(byte graphId, long nodeId, PagedNodeIdList singleQueryResult,
	                                 SortedSet<Triplet<Long,Long,Byte>> results) throws IOException,
		FlockException {
		boolean first = true;
		do {
			if (first) {
				first = false;
			} else {
				singleQueryResult = singleQueryResult.getNextPage();
			}

			for (Long destNodeId : singleQueryResult) {
				results.add(new Triplet<>(nodeId, destNodeId, graphId));
			}
		} while (singleQueryResult.hasNextPage());
	}
}
