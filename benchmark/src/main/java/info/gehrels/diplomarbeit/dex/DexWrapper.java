package info.gehrels.diplomarbeit.dex;

import com.sparsity.dex.gdb.Condition;
import com.sparsity.dex.gdb.Database;
import com.sparsity.dex.gdb.Dex;
import com.sparsity.dex.gdb.DexConfig;
import com.sparsity.dex.gdb.Graph;
import com.sparsity.dex.gdb.Objects;
import com.sparsity.dex.gdb.Session;
import com.sparsity.dex.gdb.Value;
import java.io.Closeable;
import java.io.FileNotFoundException;


public class DexWrapper implements Closeable {
  private final Dex dex;
  private final Database database;
  private final Session session;
  private final Graph graph;

  private final int nodeNameType;

  private final int edgeTypeL1;
  private final int edgeTypeL2;
  private final int edgeTypeL3;

  private final Value value;

  public DexWrapper(String storageFileName) throws FileNotFoundException {
    DexConfig dexConfig = getDexConfig();
    dexConfig.setRecoveryEnabled(true);

    dex = new Dex(dexConfig);
    database = dex.open(storageFileName, true);
    session = database.newSession();
    graph = session.getGraph();

    int nodeType = graph.findNodeTypes().iterator().next();
    nodeNameType = graph.findAttributes(nodeType).iterator().next();
    edgeTypeL1 = graph.findType("L1");
    edgeTypeL2 = graph.findType("L2");
    edgeTypeL3 = graph.findType("L3");

    value = new Value();
  }

  static DexConfig getDexConfig() {
    DexConfig config = new DexConfig();
    config.setCacheMaxSize(23040); // 22,5 G = 29 GB - 6,5 GB for the JVM
    return config;
  }

  public Session getSession() {
    return session;
  }

  public Graph getGraph() {
    return graph;
  }

  public int getNodeNameType() {
    return nodeNameType;
  }

  public long getNodeId(long nodeName) {
    try(Objects result = graph.select(nodeNameType, Condition.Equal, value.setLong(nodeName))) {
      return result.any();
    }
  }

  long getNodeName(long nodeId) {
    return graph.getAttribute(nodeId, getNodeNameType()).getLong();
  }

  @Override
  public void close() {
    session.close();
    database.close();
    dex.close();
  }

  public int getEdgeTypeL1() {
    return edgeTypeL1;
  }

  public int getEdgeTypeL2() {
    return edgeTypeL2;
  }

  public int getEdgeTypeL3() {
    return edgeTypeL3;
  }
}
