package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import com.twitter.flockdb.thrift.FlockException;
import com.twitter.flockdb.thrift.Priority;
import info.gehrels.flockDBClient.ExecutionBuilder;
import info.gehrels.flockDBClient.FlockDB;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;

public class FlockDBImporter {
	private final FileInputStream inputStream;
	private final FlockDB flockDB;
	private ExecutionBuilder executionBuilder;


	public static void main(String... args) throws IOException, FlockException {
		Stopwatch stopwatch = new Stopwatch().start();
		new FlockDBImporter(args[0]).importNow().shutdown();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	public FlockDBImporter(String sourceFile) throws IOException {
		this.inputStream = new FileInputStream(sourceFile);
		flockDB = new FlockDB("localhost", 7915);
	}

	public FlockDBImporter importNow() throws IOException, FlockException {
		executionBuilder = flockDB.batchExecution(Priority.High);
		for (GraphElement elem : new GeoffStreamParser(inputStream)) {
			if (elem instanceof Edge) {
				Edge edge = (Edge) elem;
				createEdge(edge.from, edge.to, edge.label);
			} else {
				//NodesFunction will not be stored
			}
		}

		executionBuilder.execute();
		return this;
	}

	private void createEdge(long from, long to, String label) throws IOException, FlockException {
		executionBuilder
			.add(from, Integer.valueOf(label.substring(1)), new Date().getTime(), true, to);
	}

	public void shutdown() {
		flockDB.close();
	}
}
