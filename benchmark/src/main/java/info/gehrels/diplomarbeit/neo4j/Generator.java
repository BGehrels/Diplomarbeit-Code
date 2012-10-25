package info.gehrels.diplomarbeit.neo4j;

import org.apache.commons.io.FileUtils;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import static java.lang.Math.pow;

public class Generator {
    private static Random RANDOM = new Random();
    private final MyEdgeFactory edgeFactory;
    private final MyNodeFactory nodeFactory;
    private int numberOfNodesLeft;
    private int numberOfEdgesLeft;
    private final Map<String, Integer> nodeDegrees = new HashMap<>();
    private int sumOverAllDegrees = 0;
    private final Set<String> fakedNodeDegrees = new HashSet<>();

    public Generator(MyEdgeFactory edgeFactory, MyNodeFactory nodeFactory, int numberOfNodes, int numberOfEdges) {
        this.edgeFactory = edgeFactory;
        this.nodeFactory = nodeFactory;
        this.numberOfNodesLeft = numberOfNodes;
        this.numberOfEdgesLeft = numberOfEdges;
    }

    public static void main(String[] args) throws IOException {
        for (int i = 1; i < 10; i++) {
            for (int j = minFor(i); j < 10 && j <= maxFor(i); j++) {
                int numberOfNodes = (int) Math.pow(10, i);
                int numberOfEdges = (int) Math.pow(10, j);

                System.out.println("numberOfNodes = " + numberOfNodes);
                System.out.println("numberOfEdges = " + numberOfEdges);
                generateGraph(numberOfNodes, numberOfEdges);
            }
        }

    }

    private static int minFor(int i) {
        int numberOfNodes = (int) Math.pow(10, i);
        int minimumNumberOfEdges = numberOfNodes / 2;
        return (int) Math.ceil(Math.log10(minimumNumberOfEdges));

    }

    private static int maxFor(int i) {
        int numberOfNodes = (int) Math.pow(10, i);
        int maxPossibleNumOfEdges = numberOfNodes * (numberOfNodes + 1) / 2;
        return (int) Math.floor(Math.log10(maxPossibleNumOfEdges));
    }

    private void generate() {
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


    private static void generateGraph(int numberOfNodes, int numberOfEdges) throws IOException {
        String importedDirName;
        do {
            String fileNameBase = numberOfNodes + "_" + numberOfEdges;
            String geoffFileName = fileNameBase + ".geoff";
            deleteGeofFile(geoffFileName);
            importedDirName = fileNameBase + ".imported";
            deleteImportedDirectory(importedDirName);
            generateAGeoffFile(numberOfNodes, numberOfEdges, geoffFileName);
            importGeoffFile(geoffFileName, importedDirName);
        } while (!hasDesiredPattern(importedDirName));
    }

    private static boolean hasDesiredPattern(String dbDir) {
        GraphDatabaseService graphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabase(dbDir);


        ExecutionEngine engine = new ExecutionEngine(graphDatabaseService);
        ExecutionResult result = engine
                .execute("START s=node:nodes(type=\"2\") MATCH s-[:L4]-m, m--n, n-[:L1]->s RETURN s, m, n");
        boolean hasDesiredPattern = result.iterator().hasNext();

        graphDatabaseService.shutdown();
        return hasDesiredPattern;
    }

    private static void deleteImportedDirectory(String importedDirName) {
        try {
            FileUtils.deleteDirectory(new File(importedDirName));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

    }

    private static void deleteGeofFile(String geoffFileName) {
        new File(geoffFileName).delete();
    }

    private static void importGeoffFile(String inputFileName, String outputDirName) {
        try {
            new Importer(inputFileName, outputDirName).importNow().shutdown();
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void generateAGeoffFile(int numberOfNodes, int numberOfEdges, String fileName) throws IOException {
        FileWriter writer = new FileWriter(fileName);

        MyEdgeFactory edgeFactory = new MyEdgeFactory(writer);
        new Generator(edgeFactory, new MyNodeFactory(writer), numberOfNodes, numberOfEdges).generate();
        writer.close();
    }

    public static byte getLabel() {
        int rndValue = RANDOM.nextInt(1000000000);
        double sum = 0;
        for (byte i = 1; i <= 10; i++) {
            sum += 645257983 * pow(i, -2);
            if (rndValue < sum) {
                return i;
            }
        }
        return 10;
    }

    static class MyEdgeFactory {

        private final FileWriter writer;

        public MyEdgeFactory(FileWriter writer) {
            this.writer = writer;
        }

        public Boolean createEdge(String sourceVertex, String targetVertex) {
            try {
                writer.write("(" + sourceVertex + ")-[:L" + getLabel() + "]->(" + targetVertex + ")\n");
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return true;
        }
    }


    private static class MyNodeFactory {

        private final FileWriter writer;

        public MyNodeFactory(FileWriter writer) {
            this.writer = writer;
        }


        public void createNode(String nodeId) {
            try {
                writer.write("(" + nodeId + ") {\"name\": \"" + nodeId + "\", type: " + getLabel() + "}\n");
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
