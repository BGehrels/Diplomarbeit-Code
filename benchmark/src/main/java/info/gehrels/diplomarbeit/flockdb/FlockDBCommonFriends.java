package info.gehrels.diplomarbeit.flockdb;

import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.diplomarbeit.AbstractCommonFriends;
import info.gehrels.diplomarbeit.Measurement;
import info.gehrels.flockDBClient.FlockDB;
import info.gehrels.flockDBClient.PagedNodeIdList;

import java.io.IOException;

import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.diplomarbeit.flockdb.FlockDBHelper.createFlockDB;
import static info.gehrels.flockDBClient.Direction.OUTGOING;
import static info.gehrels.flockDBClient.SelectionQuery.intersect;
import static info.gehrels.flockDBClient.SelectionQuery.simpleSelection;
import static java.lang.Integer.parseInt;

public class FlockDBCommonFriends extends AbstractCommonFriends {
	private FlockDB graphDb;

	public static void main(final String[] args) throws Exception {
		measure(new Measurement<Void>(){
			@Override
			public void execute(Void database) throws Exception {
				new FlockDBCommonFriends(createFlockDB(), parseInt(args[0])).calculateCommonFriends();
			}
		});
	}

	public FlockDBCommonFriends(FlockDB flockDB, long maxNodeId) throws IOException {
		super(maxNodeId);
		graphDb = flockDB;
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
			printCommonFriend(id1, id2, nodeId);
		}
	}
}
