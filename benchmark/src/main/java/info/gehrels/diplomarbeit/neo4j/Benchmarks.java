package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Function;
import com.twitter.flockdb.thrift.FlockException;
import com.twitter.flockdb.thrift.Priority;
import info.gehrels.flockDBClient.FlockDB;
import org.neo4j.graphalgo.CostAccumulator;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.impl.centrality.BetweennessCentrality;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPathDijkstra;
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

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Iterables.transform;
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

    private void run() throws IOException, FlockException {
        FlockDB flockDB = new FlockDB("localhost", 7915);
	    flockDB.batchExecution(Priority.High).add(1,1,1,true,2).add(1,1,1,false,2).execute();
	    System.in.read();
	    System.out.println(flockDB.getMetadata(1,1));
	    flockDB.close();

        //nodeDegreeHistogram();
        //labelHistogram();
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
            long count = count(node.getRelationships());
            Long value = histogram.get(count);
            if (value == null) {
                histogram.put(count, 1L);
            } else {
                histogram.put(count, ++value);
            }
        }


        for (Map.Entry<Long, Long> entry : histogram.entrySet()) {
            System.out.println(entry.getKey() + "\t" + entry.getValue());
        }
    }

    private void betweennessCentrality() {
        Set<Node> nodes = new HashSet<>();
        NodeNameProvider nodeNameProvider = new NodeNameProvider(123456789,
                                                                 GlobalGraphOperations.at(graphDb).getAllNodes());
        addAll(nodes, transform(nodeNameProvider, new Function<String, Node>() {
            @Override
            public Node apply(String nodeName) {
                return graphDb.index().forNodes("nodes").get("name", nodeName).getSingle();
            }
        }));

        BetweennessCentrality betweennessCentrality = new BetweennessCentrality(
                new SingleSourceShortestPathDijkstra(new Long(0), null, new CostEvaluator<Long>() {
                    @Override
                    public Long getCost(Relationship relationship, Direction direction) {
                        return Long.valueOf((String) relationship.getProperty("label")) + 1;
                    }
                }, new LongAdder(), new LongComparator(), Direction.OUTGOING, Importer.TYPE), nodes);
        for (Node node : nodes) {
            betweennessCentrality.getCentrality(node);
        }
    }

    private void findMaxNodeName() {
        long max = -1;
        for (Node node : GlobalGraphOperations.at(graphDb).getAllNodes()) {
            if (!node.hasProperty("name"))
                continue;
            max = Math.max(Long.valueOf((String) node.getProperty("name")), max);
        }

        System.out.println(max);
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

    private static class LongAdder implements CostAccumulator<Long> {
        @Override
        public Long addCosts(Long c1, Long c2) {
            return c1 + c2;
        }
    }

    private static class LongComparator implements Comparator<Long> {
        @Override
        public int compare(Long o1, Long o2) {
            return o1.compareTo(o2);
        }
    }
}
