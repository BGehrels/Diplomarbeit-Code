package info.gehrels.diplomarbeit.neo4j;

import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

public class NodeNameProvider implements Iterator<String>, Iterable<String> {
    private int numberOfReturnedNodeNames = 0;
    private final ArrayList<String> stillAvailable = new ArrayList<>();
    private Random random;

    public NodeNameProvider(int seed, Iterable<Node> nodes) {
        random = new Random(seed);
        for (Node node : nodes) {
            if (node.hasProperty("name")) {
                stillAvailable.add((String) node.getProperty("name"));
            }
        }
    }

    @Override
    public Iterator<String> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return numberOfReturnedNodeNames < 10000;
    }

    @Override
    public String next() {
        ++numberOfReturnedNodeNames;
        int randomIndex = random.nextInt(stillAvailable.size());
        String next = stillAvailable.get(randomIndex);
        stillAvailable.remove(randomIndex);
        return next;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}

