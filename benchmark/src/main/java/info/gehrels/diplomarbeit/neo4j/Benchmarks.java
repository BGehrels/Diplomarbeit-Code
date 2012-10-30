package info.gehrels.diplomarbeit.neo4j;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Traversal;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.HashMap;
import java.util.Map;

import static org.neo4j.helpers.collection.Iterables.count;

public class Benchmarks {
	private final GraphDatabaseService graphDb;

	public static void main(String[] args) throws Exception {
		new Benchmarks(args[0]).run();
	}


	public Benchmarks(String dbPath) {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		registerShutdownHook(graphDb);
	}

	private void run() {
		nodeDegreeHistogram();
		labelHistogram();
		/*Stopwatch stopwatch = new Stopwatch().start();
				friendsOfFriends(false);
				System.out.println(stopwatch.stop().elapsedMillis());
				stopwatch.reset().start();
				friendsOfFriends(true);
				System.out.println(stopwatch.stop().elapsedMillis());
				stopwatch.reset().start();
				betweennessCentrality();
				System.out.println(stopwatch.stop().elapsedMillis());*/
	}

	private void labelHistogram() {
		Map<String, Long> histogram = new HashMap<>();
		for (Relationship rel : GlobalGraphOperations.at(graphDb).getAllRelationships()) {
			String label = (String) rel.getProperty("label");
			Long value = histogram.get(label);
			if (value == null) {
				histogram.put(label, 1L);
			} else {
				histogram.put(label, ++value);
			}
		}


		for (Map.Entry<?, ?> entry : histogram.entrySet()) {
			System.out.println(entry.getKey() + "\t" + entry.getValue());
		}
	}

	private void nodeDegreeHistogram() {
		Map<Long, Long> histogram = new HashMap<>();
		for (Node node : GlobalGraphOperations.at(graphDb).getAllNodes()) {
			long count = count(node.getRelationships(Direction.BOTH));
			Long value = histogram.get(count);
			if (value == null) {
				(histogram).put(count, 1L);
			} else {
				(histogram).put(count, value + 1L);
			}
		}

		long max = 0;
		for (Long key : histogram.keySet()) {
			max = Math.max(max, key);
		}

		for (long i = 0; i <= max; i++) {
			Long value = histogram.get(i);
			System.out.println(((value == null) ? 0 : value));
		}
	}

	private void friendsOfFriends(boolean breadthFirst) {
		for (String nodeName : new NodeNameProvider(987654321, GlobalGraphOperations.at(graphDb).getAllNodes())) {
			friendsOfFriendsFor(nodeName, breadthFirst);
		}
	}

	private void friendsOfFriendsFor(String nodeName, boolean breadthFirst) {
		Node single = graphDb.index().forNodes("nodes").get("name", nodeName).getSingle();
		TraversalDescription traversal = Traversal.traversal();
		if (breadthFirst) {
			traversal = traversal.breadthFirst();
		} else {
			traversal = traversal.depthFirst();
		}
		Traverser traverser = traversal.evaluator(Evaluators.includingDepths(1, 2)).traverse(single);

		for (Node node : traverser.nodes()) {
			((String) node.getProperty("name")).length();
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
