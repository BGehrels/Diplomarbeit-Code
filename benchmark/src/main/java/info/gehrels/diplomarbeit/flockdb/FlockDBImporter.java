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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;

import static info.gehrels.flockDBClient.Direction.OUTGOING;
import static java.lang.Thread.sleep;

public class FlockDBImporter {
	public static final String IMPORT_COMPLETED_SQL = "SELECT\n"
	                                                  + "\tt1.cnt+\n"
	                                                  + "\tt2.cnt+\n"
	                                                  + "\tt3.cnt+\n"
	                                                  + "\tt4.cnt\n"
	                                                  + "FROM\n"
	                                                  + "\t(SELECT COUNT(*) as cnt FROM edges_development.forward_1_0000_edges) AS t1,\n"
	                                                  + "\t(SELECT COUNT(*) as cnt FROM edges_development.forward_2_0000_edges) AS t2,\n"
	                                                  + "\t(SELECT COUNT(*) as cnt FROM edges_development.forward_3_0000_edges) AS t3,\n"
	                                                  + "\t(SELECT COUNT(*) as cnt FROM edges_development.forward_4_0000_edges) AS t4";
	private final FileInputStream inputStream;
	private final FlockDB flockDB;

	private int numberOfImportedEdges = 0;


	public static void main(String... args) throws Exception {
		Stopwatch stopwatch = new Stopwatch().start();
		new FlockDBImporter(args[0]).importNow().shutdown();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	private void ensureImportCompleted() throws Exception {
		System.err.println("ensureImportCompleted");
		Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/edges_development", "root", "");
		while (!importCompleted(con)) {
			System.err.println("Not completed yet, waiting for 500ms");
			sleep(300);
		}
	}

	private boolean importCompleted(Connection con) throws Exception {
		System.err.println("importCompleted");

		try (Statement statement = con.createStatement();
		     ResultSet resultSet = statement.executeQuery(IMPORT_COMPLETED_SQL)) {
			resultSet.next();
			return (resultSet.getLong(1) == numberOfImportedEdges);
		} catch (Exception e) {
			return false;
		}

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
		numberOfImportedEdges++;
	}

	public void shutdown() throws Exception {
		ensureImportCompleted();
		flockDB.close();
	}
}
