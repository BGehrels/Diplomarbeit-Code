package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import info.gehrels.diplomarbeit.AbstractCommonFriends;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Traversal;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static info.gehrels.diplomarbeit.neo4j.Neo4jImporter.NAME_KEY;
import static java.lang.Integer.parseInt;

public class Neo4jCommonFriends extends AbstractCommonFriends {
	public static final DynamicRelationshipType L1 = DynamicRelationshipType.withName("L1");
	private GraphDatabaseService graphDB;

	public static void main(String[] args) throws Exception {
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jCommonFriends(Neo4jHelper.createNeo4jDatabase(args[0]), parseInt(args[1])).calculateCommonFriends();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	public Neo4jCommonFriends(GraphDatabaseService neo4jDatabase, long maxNodeId) {
		super(maxNodeId);
		this.graphDB = neo4jDatabase;
	}

	@Override
	protected void calculateCommonFriends(int id1, int id2) {
		calculateCommonFriendsUsingCypher(id1, id2);
		//calculateCommonFriendsUsingPureTraversal(id1, id2);
		//calculateCommonFriendsUsingTwoManualTraversals(id1, id2);
	}

	private void calculateCommonFriendsUsingTwoManualTraversals(int id1, int id2) {
		Node id1Node = graphDB.index().forNodes(Neo4jImporter.NODE_INDEX_NAME).get(NAME_KEY, id1).next();
		final Node id2Node = graphDB.index().forNodes(Neo4jImporter.NODE_INDEX_NAME).get(NAME_KEY, id2)
			.next();

		Set<Long> id1Friends = new TreeSet<>();
		for (Relationship rel  : id1Node.getRelationships(L1, Direction.OUTGOING)) {
			id1Friends.add((Long) rel.getEndNode().getProperty(Neo4jImporter.NAME_KEY));
		}

		Set<Long> id2Friends = new TreeSet<>();
		for (Relationship rel  : id2Node.getRelationships(L1, Direction.OUTGOING)) {
			id2Friends.add((Long) rel.getEndNode().getProperty(Neo4jImporter.NAME_KEY));
		}

		id1Friends.retainAll(id2Friends);

		System.out.println(id1Friends);
	}

	private void calculateCommonFriendsUsingPureTraversal(int id1, int id2) {
		Node id1Node = graphDB.index().forNodes(Neo4jImporter.NODE_INDEX_NAME).get(NAME_KEY, id1).next();
		final Node id2Node = graphDB.index().forNodes(Neo4jImporter.NODE_INDEX_NAME).get(NAME_KEY, id2)
			.next();


		Iterable<Path> pathTraversal =
			Traversal
				.traversal()
				.breadthFirst()
				.expand(new PathExpander<Object>() {
					@Override
					public Iterable<Relationship> expand(Path path, BranchState<Object> state) {
						return path.length() == 0 ?
							path.endNode().getRelationships(L1, Direction.OUTGOING) :
							path.endNode().getRelationships(L1, Direction.INCOMING);
					}

					@Override
					public PathExpander<Object> reverse() {
						throw new UnsupportedOperationException();
					}
				})
				.evaluator(new Evaluator() {
					public Evaluation evaluate(Path path) {
						if (path.length() < 2) {
							return Evaluation.EXCLUDE_AND_CONTINUE;
						}

						if (path.endNode().equals(id2Node)) {
							return Evaluation.INCLUDE_AND_PRUNE;
						}

						return Evaluation.EXCLUDE_AND_PRUNE;
					}
				})
				.traverse(id1Node);

		for (Path path : pathTraversal) {
			Iterator<Node> iterator = path.nodes().iterator();
			iterator.next();
			System.out.println(iterator.next().getProperty(NAME_KEY));
		}
	}

	private void calculateCommonFriendsUsingCypher(int id1, int id2) {
		ExecutionEngine cypher = new ExecutionEngine(graphDB);
		ExecutionResult result = cypher.execute("start " +
		                                        "n=node:" + Neo4jImporter.NODE_INDEX_NAME + "(" + NAME_KEY + "=\"{id1}\"), " +
		                                        "m=node:" + Neo4jImporter.NODE_INDEX_NAME + "(" + NAME_KEY + "=\"{id2}\") " +
		                                        "match m-[:L1]->x<-[:L1]-n " +
		                                        "return x.name as x ",
		                                        MapUtil.<String, Object>genericMap("id1", id1, "id2", id2));

		for (Object x : IteratorUtil.asIterable(result.columnAs("x"))) {
			System.out.println(x);
		}
	}
}
