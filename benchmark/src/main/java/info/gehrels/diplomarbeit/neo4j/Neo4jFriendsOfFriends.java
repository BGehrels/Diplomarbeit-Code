package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.kernel.Traversal;

public class Neo4jFriendsOfFriends {

	private GraphDatabaseService graphDb;

	public Neo4jFriendsOfFriends(String dbPath) {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		registerShutdownHook(graphDb);
	}

	public static void main(String... args) {
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jFriendsOfFriends(args[0]).calculateFriendsOfFriends();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	private void calculateFriendsOfFriends() {
		for (int nodeId = 0; nodeId < 1000; nodeId++) {
			calculateFriendsOfFriends(nodeId);
		}
	}

	private void calculateFriendsOfFriends(int nodeId) {
		IndexHits<Node> nodes = graphDb.index().forNodes(Neo4jImporter.NODE_INDEX_NAME)
			.get(Neo4jImporter.NAME_KEY, nodeId);

		int numberOfResults = 0;
		for (Node node : nodes) {
			numberOfResults++;
			Iterable<Node> nodesTraverser = Traversal.traversal().breadthFirst().evaluator(Evaluators.toDepth(3)).traverse(node)
				.nodes();

			for (Node traversedNode : nodesTraverser) {
				traversedNode.getProperty(Neo4jImporter.NAME_KEY);
			}

		}

		if (numberOfResults != 1) {
			throw new IllegalStateException(
				"Not exactly one result for Node " + nodeId + ": " + numberOfResults + " results");
		}
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
