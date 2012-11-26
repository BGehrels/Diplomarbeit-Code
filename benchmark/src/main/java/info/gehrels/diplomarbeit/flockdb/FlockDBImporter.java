package info.gehrels.diplomarbeit.flockdb;

import com.google.common.base.Stopwatch;
import com.twitter.flockdb.thrift.FlockException;
import com.twitter.flockdb.thrift.Priority;
import info.gehrels.diplomarbeit.Edge;
import info.gehrels.diplomarbeit.GeoffStreamParser;
import info.gehrels.diplomarbeit.GraphElement;
import info.gehrels.flockDBClient.ExecutionBuilder;
import info.gehrels.flockDBClient.FlockDB;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;

import static info.gehrels.flockDBClient.Direction.OUTGOING;

public class FlockDBImporter {
	private final FileInputStream inputStream;
	private final FlockDB flockDB;


	public static void main(String... args) throws IOException, FlockException {
		Stopwatch stopwatch = new Stopwatch().start();
		new FlockDBImporter(args[0]).importNow().shutdown();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	public FlockDBImporter(String sourceFile) throws IOException {
		this.inputStream = new FileInputStream(sourceFile);
		flockDB = new FlockDB("localhost", 7915, 1000000);
	}

	public FlockDBImporter importNow() throws IOException, FlockException {
		for (GraphElement elem : new GeoffStreamParser(inputStream)) {
			if (elem instanceof Edge) {
				Edge edge = (Edge) elem;
				createEdge(edge.from, edge.to, edge.label);
			} else {
				//NodesFunction will not be stored
			}
		}

		return this;
	}

	private void createEdge(long from, long to, String label) throws IOException, FlockException {
		ExecutionBuilder executionBuilder = flockDB.batchExecution(Priority.High);
		executionBuilder
			.add(from, Integer.valueOf(label.substring(1)), new Date().getTime(), OUTGOING, to);
		executionBuilder.execute();
	}

	public void shutdown() {
		flockDB.close();
	}
}
