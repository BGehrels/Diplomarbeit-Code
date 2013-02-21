package info.gehrels.diplomarbeit.dex;

import com.sparsity.dex.gdb.*;
import info.gehrels.diplomarbeit.AbstractReadWholeGraph;


public class DexReadWholeGraph extends AbstractReadWholeGraph {
  private final DexWrapper dexWrapper;
  private final Graph graph;

  public DexReadWholeGraph(DexWrapper dexWrapper, boolean writeToStdOut) {
    super(writeToStdOut);
    this.dexWrapper = dexWrapper;
    this.graph = this.dexWrapper.getGraph();
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

        long tailName = dexWrapper.getNodeName(tailId);
        long headId = edgeData.getHead();
        long headName = dexWrapper.getNodeName(headId);

        write(tailName, edgeType, headName);
      }

      edges.close();
    }
  }


  public static void main(String[] args) throws Exception {
    DexWrapper dexWrapper = new DexWrapper("benchmark.dex");
    DexReadWholeGraph dexReadWholeGraph = new DexReadWholeGraph(dexWrapper, true);
    dexReadWholeGraph.readWholeGraph();
    dexWrapper.close();
  }
}
