package info.gehrels.diplomarbeit;

import info.gehrels.diplomarbeit.flockdb.FlockDBBenchmarkStep;
import info.gehrels.diplomarbeit.neo4j.Neo4jBenchmarkStep;

public class RunBenchmarkStep {
	public static void main(String[] args) throws Exception {
		String inputPath = args[0];
		String dbName = args[1];
		String algorithm = args[2];

		if (dbName.equals("flockdb")) {
			new FlockDBBenchmarkStep(algorithm, inputPath).execute();
		} else if (dbName.equals("neo4j")) {
			new Neo4jBenchmarkStep(algorithm, inputPath).execute();
		}
	}
}
