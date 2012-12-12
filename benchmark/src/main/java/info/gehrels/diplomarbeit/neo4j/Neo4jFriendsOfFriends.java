package info.gehrels.diplomarbeit.neo4j;

import info.gehrels.diplomarbeit.AbstractFriendsOfFriends;
import info.gehrels.diplomarbeit.Measurement;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.Traversal;

import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.diplomarbeit.neo4j.Neo4jImporter.NAME_KEY;
import static info.gehrels.diplomarbeit.neo4j.Neo4jImporter.NODE_INDEX_NAME;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.graphdb.traversal.Evaluators.toDepth;

public class Neo4jFriendsOfFriends extends AbstractFriendsOfFriends {
	private final GraphDatabaseService graphDb;

	public static void main(final String... args) throws Exception {
		measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				new Neo4jFriendsOfFriends(Neo4jHelper.createNeo4jDatabase(args[0]), Long.parseLong(args[1]))
					.calculateFriendsOfFriends();
			}
		});
	}

	public Neo4jFriendsOfFriends(GraphDatabaseService neo4jDatabase, long maxNodeId) {
		super(maxNodeId);
		this.graphDb = neo4jDatabase;
	}

	@Override
	protected void calculateFriendsOfFriends(long startNodeId) {
		IndexHits<Node> nodes = graphDb.index().forNodes(NODE_INDEX_NAME)
			.get(NAME_KEY, startNodeId);

		for (Node node : nodes) {
			Iterable<Node> nodesTraverser = Traversal.traversal().breadthFirst()
				.relationships(withName("L1"), OUTGOING)
				.relationships(withName("L2"), OUTGOING)
				.relationships(withName("L3"), OUTGOING)
				.relationships(withName("L4"), OUTGOING)
				.evaluator(toDepth(3)).traverse(node).nodes();

			for (Node traversedNode : nodesTraverser) {
				printFriendNode(startNodeId, (Long) traversedNode.getProperty(NAME_KEY));
			}

		}
	}

}
