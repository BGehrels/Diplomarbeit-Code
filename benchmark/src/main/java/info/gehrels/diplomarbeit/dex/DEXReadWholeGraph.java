package info.gehrels.diplomarbeit.dex;

import com.sparsity.dex.gdb.*;
import info.gehrels.diplomarbeit.AbstractReadWholeGraph;
import static info.gehrels.diplomarbeit.dex.DEXHelper.closeDex;
import static info.gehrels.diplomarbeit.dex.DEXHelper.openDEX;


public class DEXReadWholeGraph extends AbstractReadWholeGraph {
  private Graph graph;
  private Integer nodeType;
  private Integer nodeIdType;

  public DEXReadWholeGraph(Session session, boolean writeToStdOut) {
    super(writeToStdOut);
    this.graph = session.getGraph();
    nodeType = graph.findNodeTypes().iterator().next();
    nodeIdType = graph.findAttributes(nodeType).iterator().next();

  }

  @Override
  public void readWholeGraph() throws Exception {
    TypeList edgeTypes = graph.findEdgeTypes();

    for (int edgeTypeId : edgeTypes) {
      String edgeType = graph.getType(edgeTypeId).getName();

      Objects edges = graph.select(edgeTypeId);
      for (long edgeId : edges) {
        EdgeData edgeData = graph.getEdgeData(edgeId);
        long tailId = edgeData.getTail();

        Value tailName = getNodeId(tailId);
        long headId = edgeData.getHead();
        Value headName = getNodeId(headId);

        write(tailName.getLong(), edgeType, headName.getLong());
      }

      edges.close();
    }
  }

  private Value getNodeId(long tailId) {
    return graph.getAttribute(tailId, nodeIdType);
  }


  public static void main(String[] args) throws Exception {
    String s = "benchmark.dex";
    Session session = openDEX(s);
    DEXReadWholeGraph dexReadWholeGraph = new DEXReadWholeGraph(session, true);
    dexReadWholeGraph.readWholeGraph();
    closeDex();
  }
}
