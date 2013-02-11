package info.gehrels.diplomarbeit.neo4j;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.util.HashMap;
import java.util.Map;

public class Neo4jHelper {
	static GraphDatabaseService createNeo4jDatabase(String dbPath) {
		Map<String, String> config = new HashMap<>();
		config.put("neostore.nodestore.db.mapped_memory", "100M");
		config.put("neostore.relationshipstore.db.mapped_memory", "4G");
		config.put("neostore.propertystore.db.mapped_memory", "500M");
		config.put("neostore.propertystore.db.strings.mapped_memory", "1M");
		config.put("neostore.propertystore.db.arrays.mapped_memory", "1M");

		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath).setConfig(config)
			.newGraphDatabase();
		registerShutdownHook(graphDb);
		return graphDb;
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
}
