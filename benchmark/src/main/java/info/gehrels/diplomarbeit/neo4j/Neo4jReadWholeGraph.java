package info.gehrels.diplomarbeit.neo4j;

import info.gehrels.diplomarbeit.AbstractReadWholeGraph;
import info.gehrels.diplomarbeit.Measurement;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.diplomarbeit.neo4j.Neo4jHelper.createNeo4jDatabase;

public class Neo4jReadWholeGraph extends AbstractReadWholeGraph {
	private final GraphDatabaseService graphDb;

	public static void main(final String... args) throws Exception {
		measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				new Neo4jReadWholeGraph(createNeo4jDatabase(args[0]), true).readWholeGraph();
			}
		});
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
