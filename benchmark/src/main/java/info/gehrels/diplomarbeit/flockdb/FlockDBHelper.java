package info.gehrels.diplomarbeit.flockdb;

import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.flockDBClient.FlockDB;
import info.gehrels.flockDBClient.PagedNodeIdList;

import java.io.IOException;

import static info.gehrels.flockDBClient.Direction.OUTGOING;
import static info.gehrels.flockDBClient.SelectionQuery.simpleSelection;
import static info.gehrels.flockDBClient.SelectionQuery.union;
import static java.lang.Runtime.getRuntime;

public class FlockDBHelper {
	static Iterable<Long> getAllOutgoingRelationshipsFor(FlockDB graphDb, long nodeId) throws IOException,
		FlockException {
		final PagedNodeIdList result = graphDb.select(
			union(
				union(
					union(
						simpleSelection(nodeId, 1, OUTGOING),
						simpleSelection(nodeId, 2, OUTGOING)
					),
					simpleSelection(nodeId, 3, OUTGOING)
				),
				simpleSelection(nodeId, 4, OUTGOING)
			)
		).execute().get(0);

		return new NonPagedResultList(result);
	}

	static FlockDB createFlockDB() throws IOException {
		FlockDB graphDb = new FlockDB("localhost", 7915, 1000000);
		registerShutdownHook(graphDb);
		return graphDb;
	}

	private static void registerShutdownHook(final FlockDB graphDb) {
		getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.close();
			}
		});
	}

}
