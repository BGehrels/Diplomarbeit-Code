package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import info.gehrels.diplomarbeit.AbstractBenchmarkStep;
import org.neo4j.graphdb.GraphDatabaseService;

public class Neo4jBenchmarkStep extends AbstractBenchmarkStep {

	public static final String DB_FOLDER = "neo4jDB";

	public Neo4jBenchmarkStep(String algorithm, String inputPath) {
		super(algorithm, inputPath);
	}

	@Override
	protected void runImporter(String inputPath) throws Exception {
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jImporter(inputPath, DB_FOLDER).importNow().shutdown();
		stopwatch.stop();
		System.err.println(stopwatch);
	}

	@Override
	protected void readWholeGraph() {
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jReadWholeGraph(Neo4jHelper.createNeo4jDatabase(DB_FOLDER)).readWholeGraph();
		stopwatch.stop();
		System.err.println(stopwatch);
	}

	@Override
	protected void calcSCC() throws Exception {
		GraphDatabaseService neo4jDatabase = Neo4jHelper.createNeo4jDatabase(DB_FOLDER);
		new Neo4jReadWholeGraph(neo4jDatabase).readWholeGraph();
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jStronglyConnectedComponents(neo4jDatabase).calculateStronglyConnectedComponents();
		stopwatch.stop();
		System.err.println(stopwatch);
	}

	@Override
	protected void calcFoF() throws Exception {
		GraphDatabaseService neo4jDatabase = Neo4jHelper.createNeo4jDatabase(DB_FOLDER);
		new Neo4jReadWholeGraph(neo4jDatabase).readWholeGraph();
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jFriendsOfFriends(neo4jDatabase, maxNodeId).calculateFriendsOfFriends();
		stopwatch.stop();
		System.err.println(stopwatch);
	}

	@Override
	protected void calcCommonFriends() throws Exception {
		GraphDatabaseService neo4jDatabase = Neo4jHelper.createNeo4jDatabase(DB_FOLDER);
		new Neo4jReadWholeGraph(neo4jDatabase).readWholeGraph();
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jCommonFriends(neo4jDatabase, maxNodeId).calculateCommonFriends();
		stopwatch.stop();
		System.err.println(stopwatch);
	}

	@Override
	protected void calcRegularPathQueries() throws Exception {
		GraphDatabaseService neo4jDatabase = Neo4jHelper.createNeo4jDatabase(DB_FOLDER);
		new Neo4jReadWholeGraph(neo4jDatabase).readWholeGraph();
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jRegularPathQuery(neo4jDatabase, maxNodeId).calculateRegularPaths();
		stopwatch.stop();
		System.err.println(stopwatch);
	}

}
