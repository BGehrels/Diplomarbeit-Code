package info.gehrels.diplomarbeit.neo4j;

import info.gehrels.diplomarbeit.AbstractBenchmarkStep;
import info.gehrels.diplomarbeit.Measurement;
import org.neo4j.graphdb.GraphDatabaseService;

import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.diplomarbeit.neo4j.Neo4jHelper.createNeo4jDatabase;

public class Neo4jBenchmarkStep extends AbstractBenchmarkStep<GraphDatabaseService> {

	public static final String DB_FOLDER = "neo4jDB";

	public Neo4jBenchmarkStep(String algorithm, String inputPath) {
		super(algorithm, inputPath);
	}

	@Override
	protected void runImporter(final String inputPath) throws Exception {
		measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				Neo4jImporter neo4jImporter = new Neo4jImporter(inputPath, DB_FOLDER);
				neo4jImporter.importNow();
				neo4jImporter.shutdown();
			}
		});
	}

	@Override
	protected void readWholeGraph() throws Exception {
		measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				new Neo4jReadWholeGraph(createNeo4jDatabase(DB_FOLDER), true).readWholeGraph();
			}
		}, null);
	}

	@Override
	protected void calcSCC() throws Exception {
		warmUpDatabaseAndMeasure(new Measurement<GraphDatabaseService>() {
			public void execute(GraphDatabaseService neo4jDatabase) throws Exception {
				new Neo4jStronglyConnectedComponents(neo4jDatabase).calculateStronglyConnectedComponents();
			}

		});
	}

	@Override
	protected void calcFoF() throws Exception {
		warmUpDatabaseAndMeasure(new Measurement<GraphDatabaseService>() {
			public void execute(GraphDatabaseService neo4jDatabase) throws Exception {
				new Neo4jFriendsOfFriends(neo4jDatabase, maxNodeId).calculateFriendsOfFriends();
			}
		});
	}

	@Override
	protected void calcCommonFriends() throws Exception {
		warmUpDatabaseAndMeasure(new Measurement<GraphDatabaseService>() {
			public void execute(GraphDatabaseService neo4jDatabase) throws Exception {
				new Neo4jCommonFriends(neo4jDatabase, maxNodeId).calculateCommonFriends();
			}
		});
	}

	@Override
	protected void calcRegularPathQueries() throws Exception {
		warmUpDatabaseAndMeasure(new Measurement<GraphDatabaseService>() {
			public void execute(GraphDatabaseService neo4jDatabase) throws Exception {
				new Neo4jRegularPathQuery(neo4jDatabase, maxNodeId).calculateRegularPaths();
			}
		});
	}

	@Override
	protected GraphDatabaseService createAndWarmUpDatabase() throws Exception {
		GraphDatabaseService neo4jDatabase = createNeo4jDatabase(DB_FOLDER);
		new Neo4jReadWholeGraph(neo4jDatabase, false).readWholeGraph();
		return neo4jDatabase;
	}

}
