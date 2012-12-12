package info.gehrels.diplomarbeit.neo4j;

import info.gehrels.diplomarbeit.AbstractStronglyConnectedComponentsCalculator;
import info.gehrels.diplomarbeit.Measurement;
import info.gehrels.diplomarbeit.PrefetchingIterableIterator;
import info.gehrels.diplomarbeit.TransformingIteratorWrapper;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.Iterator;

import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.diplomarbeit.neo4j.Neo4jHelper.createNeo4jDatabase;

public class Neo4jStronglyConnectedComponents
	extends AbstractStronglyConnectedComponentsCalculator<GraphDatabaseService, Node> {
	public static void main(final String... args) throws Exception {
		measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				new Neo4jStronglyConnectedComponents(createNeo4jDatabase(args[0])).calculateStronglyConnectedComponents();
			}
		});
	}

	public Neo4jStronglyConnectedComponents(GraphDatabaseService neo4jDatabase) {
		super(neo4jDatabase);
	}

	@Override
	protected Iterable<Node> getAllNodes() {
		return new PrefetchingIterableIterator<Node>() {
			private final Iterator<Node> iterator = GlobalGraphOperations.at(graphDB).getAllNodes().iterator();

			@Override
			protected void ensureNextIsFetched() {
				if (next == null && iterator.hasNext()) {
					next = iterator.next();
				}

				if (next != null && next.getId() == 0) {
					next = null;
					ensureNextIsFetched();
				}
			}
		};
	}

	protected long getNodeName(Node endNode) {
		return (Long) endNode.getProperty(Neo4jImporter.NAME_KEY);
	}

	protected Iterable<Node> getOutgoingIncidentNodes(Node node) {
		final Iterator<Relationship> relationships = node.getRelationships(Direction.OUTGOING).iterator();
		return new TransformingIteratorWrapper<Relationship, Node>(relationships) {
			@Override
			protected Node calculateNext(Relationship next) {
				return next.getEndNode();
			}
		};
	}

}
