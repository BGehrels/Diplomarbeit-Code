package info.gehrels.diplomarbeit.dex;

import com.sparsity.dex.gdb.*;
import java.io.Closeable;
import java.io.FileNotFoundException;


public class DexWrapper implements Closeable {
  private final Dex dex;
  private final Database database;
  private final Session session;
  private final Graph graph;

  private final int nodeType;
  private final int nodeNameType;
  private final int edgeTypeL1;

  private final Value value;

  public DexWrapper(String storageFileName) throws FileNotFoundException {
    DexConfig dexConfig = new DexConfig();

    dex = new Dex(dexConfig);
    database = dex.open(storageFileName, true);
    session = database.newSession();
    graph = session.getGraph();

    nodeType = graph.findNodeTypes().iterator().next();
    nodeNameType = graph.findAttributes(nodeType).iterator().next();
    edgeTypeL1 = graph.findType("L1");

    value = new Value();
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
}
