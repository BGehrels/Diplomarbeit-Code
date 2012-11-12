package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Traversal;

import java.util.Iterator;

import static info.gehrels.diplomarbeit.neo4j.Neo4jImporter.NAME_KEY;
import static java.lang.Integer.parseInt;

public class Neo4jCommonFriends extends AbstractCommonFriends {
	private GraphDatabaseService graphDB;

	public static void main(String[] args) throws Exception {
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jCommonFriends(args[0], parseInt(args[1])).calculateCommonFriends();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	public Neo4jCommonFriends(String neo4jPath, int maxNodeId) {
		super(maxNodeId);
		this.graphDB = Neo4jHelper.createNeo4jDatabase(neo4jPath);
	}

	@Override
	protected void calculateCommonFriends(int id1, int id2) {
		calculateCommonFriendsUsingCypher(id1, id2);
		calculateCommonFriendsUsingPureTraversal(id1, id2);
	}

	private void calculateCommonFriendsUsingPureTraversal(int id1, int id2) {
		Node id1Node = graphDB.index().forNodes(Neo4jImporter.NODE_INDEX_NAME).get(NAME_KEY, id1).next();
		final Node id2Node = graphDB.index().forNodes(Neo4jImporter.NODE_INDEX_NAME).get(NAME_KEY, id2)
			.next();


		final RelationshipType l1 = DynamicRelationshipType.withName("L1");
		Iterable<Path> pathTraversal =
			Traversal
				.traversal()
				.breadthFirst()
				.expand(new PathExpander<Object>() {
					@Override
					public Iterable<Relationship> expand(Path path, BranchState<Object> state) {
						return path.length() == 0 ?
							path.endNode().getRelationships(l1, Direction.OUTGOING) :
							path.endNode().getRelationships(l1, Direction.INCOMING);
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
		ExecutionResult result = cypher.execute("start n=node({id1}), m=node({id2}) "
		                                        + "match m-[:L1]->x<-[:L1]-n "
		                                        + "return x.name as x ",
		                                        MapUtil.<String, Object>genericMap("id1", id1, "id2", id2));

		for (Object x : IteratorUtil.asIterable(result.columnAs("x"))) {
			System.out.println(x);
		}
	}
}
