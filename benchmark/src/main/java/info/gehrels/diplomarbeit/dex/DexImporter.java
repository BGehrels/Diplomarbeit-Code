package info.gehrels.diplomarbeit.dex;

import com.sparsity.dex.gdb.*;
import info.gehrels.diplomarbeit.CachingImporter;
import info.gehrels.diplomarbeit.Node;
import java.util.HashMap;
import java.util.Map;


public class DexImporter extends CachingImporter<Long> {
  public static final Value VALUE = new Value();

  private final Graph graph;
  private final int nodeType;

  private final int nodeIdType;
  private final Map<String, Integer> EDGE_LABEL_TO_EDGE_TYPE_ID = new HashMap<>();
  private final Session session;
  private final Database db;
  private final Dex dex;

  public DexImporter(String inputPath, String storageFileName) throws Exception {
    super(inputPath);


    DexConfig config = DexWrapper.getDexConfig();
    config.setRecoveryEnabled(false);

    dex = new Dex(config);
    db = dex.create(storageFileName, "Benchmark");
    session = db.newSession();
    graph = session.getGraph();
    session.begin();

    nodeType = graph.newNodeType("BENCHMARK_NODE");
    nodeIdType = graph.newAttribute(nodeType, "ID", DataType.Long, AttributeKind.Unique);

    // TODO: Batch import?
    EDGE_LABEL_TO_EDGE_TYPE_ID.put("L1", graph.newEdgeType("L1", true, true)); // TODO: Will ich wirklich das zweite true?
    EDGE_LABEL_TO_EDGE_TYPE_ID.put("L2", graph.newEdgeType("L2", true, true));
    EDGE_LABEL_TO_EDGE_TYPE_ID.put("L3", graph.newEdgeType("L3", true, true));
    EDGE_LABEL_TO_EDGE_TYPE_ID.put("L4", graph.newEdgeType("L4", true, true));
  }

  @Override
  protected void createEdgeBetweenCachedNodes(Long from, Long to, String label) throws Exception {
    graph.newEdge(EDGE_LABEL_TO_EDGE_TYPE_ID.get(label), from, to);
  }

  @Override
  protected Long createNodeForCache(Node node) {
    long nodeId = graph.newNode(nodeType);
    graph.setAttribute(nodeId, nodeIdType, VALUE.setLong(node.id));
    return nodeId;
  }


  public void shutdown() {
    session.commit();
    session.close();
    db.close();
    dex.close();
  }
}
