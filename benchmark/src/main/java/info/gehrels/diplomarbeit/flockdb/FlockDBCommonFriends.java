package info.gehrels.diplomarbeit.flockdb;

import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.diplomarbeit.AbstractCommonFriends;
import info.gehrels.flockDBClient.FlockDB;
import info.gehrels.flockDBClient.PagedNodeIdList;

import java.io.IOException;

import static info.gehrels.flockDBClient.Direction.OUTGOING;
import static info.gehrels.flockDBClient.SelectionQuery.intersect;
import static info.gehrels.flockDBClient.SelectionQuery.simpleSelection;

public class FlockDBCommonFriends extends AbstractCommonFriends {
	private FlockDB graphDb;

	public static void main(String[] args) throws Exception {
		new FlockDBCommonFriends(Integer.parseInt(args[0])).calculateCommonFriends();
	}

	public FlockDBCommonFriends(long maxNodeId) throws IOException {
		super(maxNodeId);
		graphDb = FlockDBHelper.createFlockDB();
	}

	@Override
	protected void calculateCommonFriends(int id1, int id2) throws IOException, FlockException {
		PagedNodeIdList nodes = graphDb
			.select(
				intersect(
					simpleSelection(id1, 1, OUTGOING),
					simpleSelection(id2, 1, OUTGOING))
			).execute().get(0);


		for (Long nodeId : new NonPagedResultList(nodes)) {
		}
	}
}
