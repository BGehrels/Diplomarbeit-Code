package info.gehrels.diplomarbeit.flockdb;

import com.google.common.base.Stopwatch;
import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.diplomarbeit.Triplet;
import info.gehrels.flockDBClient.FlockDB;
import info.gehrels.flockDBClient.PagedNodeIdList;
import info.gehrels.flockDBClient.SelectionQuery;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static info.gehrels.flockDBClient.Direction.OUTGOING;

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

	// TODO: Statt dessen vielleicht UNION Query? oder mehrere Parallele?
	private FlockDBReadWholeGraph readWholeGraph() throws IOException, FlockException {
		for (long nodeId = 0; nodeId <= maxNodeId; nodeId++) {
			SortedSet<Triplet<Long, Long, Byte>> results = new TreeSet<>();
			for (byte graphId = 1; graphId <= 15; graphId++) {
				readAllEdgesForNode(graphId, nodeId);
			}

			printOutResults(results);
		}
		System.out.println(numberOfResults);
		return this;
	}

	private void printOutResults(SortedSet<Triplet<Long, Long, Byte>> results) {
		for (Triplet result : results) {
			System.out.println(result.elem1 + ", L" + result.elem3 + ", " + result.elem2);
			numberOfResults++;
		}
	}

	private void readAllEdgesForNode(byte graphId, long nodeId) throws
		IOException, FlockException {
		List<PagedNodeIdList> result
			= flockDB
			.select(SelectionQuery.simpleSelection(nodeId, graphId, OUTGOING))
			.execute();
		for (long singleQueryResult : new NonPagedResultList(result.get(0))) {
			System.out.println(nodeId + ", L" + graphId + ", " + singleQueryResult);
		}
	}

}
