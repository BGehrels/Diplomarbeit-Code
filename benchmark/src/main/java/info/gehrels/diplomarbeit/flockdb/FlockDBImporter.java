package info.gehrels.diplomarbeit.flockdb;

import com.twitter.flockdb.thrift.FlockException;
import com.twitter.flockdb.thrift.Priority;
import info.gehrels.diplomarbeit.AbstractImporter;
import info.gehrels.diplomarbeit.Edge;
import info.gehrels.diplomarbeit.Measurement;
import info.gehrels.diplomarbeit.Node;
import info.gehrels.flockDBClient.ExecutionBuilder;
import info.gehrels.flockDBClient.FlockDB;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.diplomarbeit.flockdb.FlockDBHelper.createFlockDB;
import static info.gehrels.flockDBClient.Direction.OUTGOING;
import static java.lang.Thread.sleep;
import static java.sql.DriverManager.getConnection;


public class FlockDBImporter extends AbstractImporter {
  public static final String IMPORT_COMPLETED_SQL = "SELECT\n" +
    "\tt1.cnt+\n" +
    "\tt2.cnt+\n" +
    "\tt3.cnt+\n" +
    "\tt4.cnt\n" +
    "FROM\n" +
    "\t(SELECT COUNT(*) as cnt FROM edges_development.forward_1_0000_edges) AS t1,\n" +
    "\t(SELECT COUNT(*) as cnt FROM edges_development.forward_2_0000_edges) AS t2,\n" +
    "\t(SELECT COUNT(*) as cnt FROM edges_development.forward_3_0000_edges) AS t3,\n" +
    "\t(SELECT COUNT(*) as cnt FROM edges_development.forward_4_0000_edges) AS t4";

  private final FlockDB flockDB;

  private int numberOfImportedEdges = 0;


  public static void main(final String... args) throws Exception {
    measure(new Measurement<Void>() {
        @Override
        public void execute(Void database) throws Exception {
          new FlockDBImporter(args[0]).importNow();
        }
      });
  }

  public FlockDBImporter(String sourceFile) throws Exception {
    super(sourceFile);
    flockDB = createFlockDB();
  }

  @Override
  protected void createEdge(Edge edge) throws IOException, FlockException {
    ExecutionBuilder executionBuilder = flockDB.batchExecution(Priority.High);
    executionBuilder.add(edge.from, Integer.valueOf(edge.label.substring(1)), new Date().getTime(), OUTGOING, edge.to);
    executionBuilder.execute();
    numberOfImportedEdges++;
  }

  @Override
  protected void createNode(Node node) {
  }

  public void ensureImportCompleted() throws Exception {
    System.err.println("ensureImportCompleted");

    try(Connection con = getConnection("jdbc:mysql://localhost:3306/edges_development", "root", "")) {
      for (long numOfEdges = 0; numOfEdges < numberOfImportedEdges; numOfEdges = getNumberOfImportedEdges(con)) {
        System.err.println(numOfEdges + " edges completed yet, waiting...");
        sleep(300);
      }
    }
  }

  private long getNumberOfImportedEdges(Connection con) throws Exception {
    try(Statement statement = con.createStatement();
      ResultSet resultSet = statement.executeQuery(IMPORT_COMPLETED_SQL)) {
      resultSet.next();
      return resultSet.getLong(1);
    }
  }

}
