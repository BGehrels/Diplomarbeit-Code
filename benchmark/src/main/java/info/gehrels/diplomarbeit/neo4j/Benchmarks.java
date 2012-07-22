package info.gehrels.diplomarbeit.neo4j;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Traversal;

public class Benchmarks {
    private final GraphDatabaseService graphDb;

    public static void main(String[] args) {
        new Benchmarks(args[0]).run();
    }

    public Benchmarks(String dbPath) {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
        registerShutdownHook(graphDb);
    }

    private void run() {
        friendsOfFriends();
    }

    private void friendsOfFriends() {
        Node single = graphDb.index().forNodes("nodes").get("name", "12345").getSingle();
        Traverser traverser = Traversal.traversal().evaluator(Evaluators.includingDepths(1,2)).traverse(single);

        for (Node node : traverser.nodes()) {
            System.out.println(node.getProperty("name"));
        }
    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }
}
