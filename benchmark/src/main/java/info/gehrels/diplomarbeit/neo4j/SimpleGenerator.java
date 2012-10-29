package info.gehrels.diplomarbeit.neo4j;

import info.gehrels.diplomarbeit.neo4j.GraphGenerationBatch.MyEdgeFactory;
import info.gehrels.diplomarbeit.neo4j.GraphGenerationBatch.MyNodeFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

public class SimpleGenerator implements Generator {
	static Random RANDOM = new Random();
	private final MyEdgeFactory edgeFactory;
    private final MyNodeFactory nodeFactory;
    private int numberOfNodesLeft;
    private int numberOfEdgesLeft;
    private final Map<String, Integer> nodeDegrees = new HashMap<>();
    private int sumOverAllDegrees = 0;
    private final Set<String> fakedNodeDegrees = new HashSet<>();

    public SimpleGenerator(MyEdgeFactory edgeFactory, MyNodeFactory nodeFactory, int numberOfNodes, int numberOfEdges) {
        this.edgeFactory = edgeFactory;
        this.nodeFactory = nodeFactory;
        this.numberOfNodesLeft = numberOfNodes;
        this.numberOfEdgesLeft = numberOfEdges;
    }

	@Override
	public void generate() {
        createNode("" + 0);

        int i = 1;
        while (numberOfNodesLeft > 0) {
            String newNode = "" + i;
            createNode(newNode);
            int numberOfEdgesForThisNode = numberOfEdgesLeft / (numberOfNodesLeft + 1);
            for (int j = 0; j < numberOfEdgesForThisNode && j < i; j++) {
                addEdge(newNode);
                numberOfEdgesLeft--;
            }
            i++;
        }

    }

    private void createNode(String node) {
        nodeDegrees.put(node, 1);
        numberOfNodesLeft--;
        sumOverAllDegrees++;
        fakedNodeDegrees.add(node);
        nodeFactory.createNode(node);
    }

    private void addEdge(String from) {
        int rndValue = RANDOM.nextInt(sumOverAllDegrees);


        int localSum = 0;
        for (Entry<String, Integer> nodeToDegree : nodeDegrees.entrySet()) {
            localSum += nodeToDegree.getValue();
            if (rndValue < localSum) {
                addEdge(from, nodeToDegree.getKey());
                return;
            }
        }
    }

    private void addEdge(String from, String to) {
        if (RANDOM.nextInt(2) == 0) {

            edgeFactory.createEdge(from, to);
        } else {
            edgeFactory.createEdge(to, from);
        }

        incNodeDegree(from);
        incNodeDegree(to);
    }

    private void incNodeDegree(String from) {
        if (fakedNodeDegrees.contains(from)) {
            fakedNodeDegrees.remove(from);
        } else {
            Integer integer = nodeDegrees.get(from);
            nodeDegrees.put(from, integer + 1);
            sumOverAllDegrees++;
        }
    }


}
