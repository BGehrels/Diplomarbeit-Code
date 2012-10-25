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
import java.util.Random;

import static java.lang.Math.pow;

public class GraphGenerationBatch {
	static Random RANDOM = new Random();

	public static void main(String[] args) throws IOException {
	    for (int i = 1; i < 20; i++) {
	        for (int j = minFor(i); j < 10 && j <= maxFor(i); j++) {
	            int numberOfNodes = (int) Math.pow(2, i);
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

	private static void generateGraph(int numberOfNodes, int numberOfEdges) throws IOException {
	    String importedDirName;
	    //do {
	        String fileNameBase = numberOfNodes + "_" + numberOfEdges;
	        String geoffFileName = fileNameBase + ".geoff";
	        deleteGeofFile(geoffFileName);
	        //importedDirName = fileNameBase + ".imported";
	        //deleteImportedDirectory(importedDirName);
	        generateAGeoffFile(numberOfNodes, numberOfEdges, geoffFileName);
	        //importGeoffFile(geoffFileName, importedDirName);
	    //} while (!hasDesiredPattern(importedDirName));
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
	    new SimpleGenerator(edgeFactory, new MyNodeFactory(writer), numberOfNodes, numberOfEdges).generate();
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

	static class MyNodeFactory {

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
}
