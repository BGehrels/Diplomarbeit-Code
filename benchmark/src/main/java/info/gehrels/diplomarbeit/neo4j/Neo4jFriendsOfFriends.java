package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import com.twitter.flockdb.thrift.FlockException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.kernel.Traversal;

import java.io.IOException;

public class Neo4jFriendsOfFriends extends AbstractFriendsOfFriends {

	private GraphDatabaseService graphDb;

	public static void main(String... args) throws IOException, FlockException {
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jFriendsOfFriends(args[0], Long.parseLong(args[1])).calculateFriendsOfFriends();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	public Neo4jFriendsOfFriends(String dbPath, long maxNodeId) {
		super(maxNodeId);
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		registerShutdownHook(graphDb);
	}

	@Override
	protected void calculateFriendsOfFriends(long startNodeId) {
		IndexHits<Node> nodes = graphDb.index().forNodes(Neo4jImporter.NODE_INDEX_NAME)
			.get(Neo4jImporter.NAME_KEY, startNodeId);

		for (Node node : nodes) {
			Iterable<Node> nodesTraverser = Traversal.traversal().breadthFirst()
				.relationships(DynamicRelationshipType.withName("L1"), Direction.OUTGOING)
				.relationships(DynamicRelationshipType.withName("L2"), Direction.OUTGOING)
				.relationships(DynamicRelationshipType.withName("L3"), Direction.OUTGOING)
				.relationships(DynamicRelationshipType.withName("L4"), Direction.OUTGOING)
				.evaluator(Evaluators.toDepth(3)).traverse(node).nodes();

			for (Node traversedNode : nodesTraverser) {
				traversedNode.getProperty(Neo4jImporter.NAME_KEY);
			}

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
