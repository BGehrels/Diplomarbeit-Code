package info.gehrels.diplomarbeit.neo4j;

import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.flockDBClient.FlockDB;
import info.gehrels.flockDBClient.PagedNodeIdList;

import java.io.IOException;
import java.util.Iterator;

import static info.gehrels.flockDBClient.SelectionQuery.intersect;
import static info.gehrels.flockDBClient.SelectionQuery.simpleSelection;

public class FlockDBCommonFriends {
	private FlockDB graphDb;
	private final long maxNodeId;

	public static void main(String[] args) throws IOException, FlockException {
		new FlockDBCommonFriends(Integer.parseInt(args[0])).calculateCommonFriends();
	}

	public FlockDBCommonFriends(long maxNodeId) throws IOException {
		this.maxNodeId = maxNodeId;
		graphDb = FlockDBHelper.createFlockDB();
	}

	private void calculateCommonFriends() throws FlockException, IOException {
		Iterator<Integer> id2Generator = new RandomNodeIdGenerator(maxNodeId, 1000).iterator();
		for (Integer id1 : new RandomNodeIdGenerator(maxNodeId, 1000)) {
			calculateCommonFriends(id1, id2Generator.next());
		}
	}

	private void calculateCommonFriends(int id1, int id2) throws IOException, FlockException {
		PagedNodeIdList nodes = graphDb.select(intersect(simpleSelection(id1, 1, true), simpleSelection(id2, 1, true)))
			.execute().get(0);


		for (Long nodeId : new NonPagedResultList(nodes)) {
		}
	}
}
