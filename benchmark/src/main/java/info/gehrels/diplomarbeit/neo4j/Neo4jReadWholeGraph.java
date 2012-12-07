package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import info.gehrels.diplomarbeit.AbstractReadWholeGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

public class Neo4jReadWholeGraph extends AbstractReadWholeGraph {
	private final GraphDatabaseService graphDb;

	public static void main(String... args) {
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jReadWholeGraph(Neo4jHelper.createNeo4jDatabase(args[0]), true).readWholeGraph();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	public Neo4jReadWholeGraph(GraphDatabaseService neo4jDatabase, boolean writeToStdOut) {
		super(writeToStdOut);
		graphDb = neo4jDatabase;

	}

	@Override
	public void readWholeGraph() {
		for (Relationship rel : GlobalGraphOperations.at(graphDb).getAllRelationships()) {
			write(
				rel.getStartNode().getProperty(Neo4jImporter.NAME_KEY),
				rel.getType(),
				rel.getEndNode().getProperty(Neo4jImporter.NAME_KEY)
			);
		}
	}

}
