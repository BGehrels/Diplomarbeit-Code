package info.gehrels.diplomarbeit.neo4j;

import info.gehrels.diplomarbeit.Measurement;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.HashMap;
import java.util.Map;

import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.diplomarbeit.neo4j.Neo4jHelper.createNeo4jDatabase;

public class Neo4jNodeDegreeHistogram {
	private final GraphDatabaseService graphDb;
	private final Map<Long, Long> histogram = new HashMap<>();
	long maxDegree = 0;

	public static void main(final String... args) throws Exception {
		measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				new Neo4jNodeDegreeHistogram(createNeo4jDatabase(args[0])).writeOutHistogram();
			}
		});
	}

	public Neo4jNodeDegreeHistogram(GraphDatabaseService neo4jDatabase) {
		graphDb = neo4jDatabase;

	}

	public void writeOutHistogram() {
		for (Node node : GlobalGraphOperations.at(graphDb).getAllNodes()) {
			long nodeDegree = 0;
			for (Object o : node.getRelationships()) {
				nodeDegree++;
			}

			addToHistogram(nodeDegree);
		}

		for (long nodeDegree = maxDegree; nodeDegree >= 0; nodeDegree--) {
			Long numberOfNodes = histogram.get(nodeDegree);
			if (numberOfNodes == null) {
				numberOfNodes = 0L;
			}
			System.out.println(nodeDegree + ": " + numberOfNodes);
		}
	}

	private void addToHistogram(long nodeDegree) {
		Long numberOfNodes = this.histogram.get(nodeDegree);
		if (numberOfNodes == null) {
			numberOfNodes = 0L;
		}

		numberOfNodes++;
		this.histogram.put(nodeDegree, numberOfNodes);
		maxDegree = Math.max(nodeDegree, maxDegree);
	}

}
