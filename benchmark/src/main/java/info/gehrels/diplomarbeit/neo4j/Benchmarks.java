package info.gehrels.diplomarbeit.neo4j;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

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
