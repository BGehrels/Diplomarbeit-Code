package info.gehrels.diplomarbeit.neo4j;

import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.flockDBClient.FlockDB;
import info.gehrels.flockDBClient.PagedNodeIdList;

import java.io.IOException;

import static info.gehrels.flockDBClient.SelectionQuery.simpleSelection;
import static info.gehrels.flockDBClient.SelectionQuery.union;

public class FlockDBHelper {
	static Iterable<Long> getAllOutgoingRelationshipsFor(FlockDB graphDb, long nodeId) throws IOException,
		FlockException {
		final PagedNodeIdList result = graphDb.select(
			union(
				union(
					union(
						simpleSelection(nodeId, 1, true),
						simpleSelection(nodeId, 2, true)
					),
					simpleSelection(nodeId, 3, true)
				),
				simpleSelection(nodeId, 4, true)
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
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.close();
			}
		});
	}

}
