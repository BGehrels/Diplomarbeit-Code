package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

public class Neo4jReadWholeGraph {

	private GraphDatabaseService graphDb;

	public static void main(String... args) {
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jReadWholeGraph(Neo4jHelper.createNeo4jDatabase(args[0])).readWholeGraph(true);
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	public Neo4jReadWholeGraph(GraphDatabaseService neo4jDatabase) {
		graphDb = neo4jDatabase;
	}

	public Neo4jReadWholeGraph readWholeGraph(boolean writeToSystemOut) {
		for (Relationship rel : GlobalGraphOperations.at(graphDb).getAllRelationships()) {
			String output =
				rel.getStartNode().getProperty(Neo4jImporter.NAME_KEY) + ", " + rel.getType() + ", " + rel.getEndNode()
					.getProperty(Neo4jImporter.NAME_KEY);
			if (writeToSystemOut) {
				System.out.println(output);
			}
		}
		return this;
	}
}
