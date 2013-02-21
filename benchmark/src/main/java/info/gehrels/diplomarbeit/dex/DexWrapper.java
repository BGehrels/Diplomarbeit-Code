package info.gehrels.diplomarbeit.dex;

import com.sparsity.dex.gdb.*;
import java.io.Closeable;
import java.io.FileNotFoundException;


public class DexWrapper implements Closeable {
  private final Dex dex;
  private final Database database;
  private final Session session;
  private final Graph graph;
  private final int nodeIdType;

  public DexWrapper(String storageFileName) throws FileNotFoundException {
    DexConfig dexConfig = new DexConfig();

    dex = new Dex(dexConfig);
    database = dex.open(storageFileName, true);
    session = database.newSession();
    graph = session.getGraph();

    int nodeType = graph.findNodeTypes().iterator().next();
    nodeIdType = graph.findAttributes(nodeType).iterator().next();
  }

  public Session getSession() {
    return session;
  }

  public Graph getGraph() {
    return graph;
  }

  public int getNodeIdType() {
    return nodeIdType;
  }

  long getNodeName(long tailId) {
    return graph.getAttribute(tailId, getNodeIdType()).getLong();
  }

  @Override
  public void close() {
    session.close();
    database.close();
    dex.close();
  }
}
