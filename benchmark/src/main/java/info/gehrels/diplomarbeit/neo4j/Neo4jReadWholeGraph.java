package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

public class Neo4jReadWholeGraph {

	private GraphDatabaseService graphDb;

	public static void main(String... args) {
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jReadWholeGraph(args[0]).readWholeGraph();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	private Neo4jReadWholeGraph readWholeGraph() {
		for (Relationship relationship : GlobalGraphOperations.at(graphDb).getAllRelationships()) {
			relationship.getStartNode().getProperty(Neo4jImporter.NAME_KEY);
			relationship.getEndNode().getProperty(Neo4jImporter.NAME_KEY);
			for (String key : relationship.getPropertyKeys()) {
				relationship.getProperty(key);
			}
		}
		return this;
	}

	public Neo4jReadWholeGraph(String dbPath) {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		registerShutdownHook(graphDb);
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
