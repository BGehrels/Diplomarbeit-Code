package info.gehrels.diplomarbeit.dex;

import com.sparsity.dex.gdb.Graph;
import com.sparsity.dex.gdb.Objects;
import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.diplomarbeit.AbstractRegularPathQuery;
import java.io.IOException;
import static com.sparsity.dex.gdb.EdgesDirection.Ingoing;


public class DexRegularPathQueries extends AbstractRegularPathQuery {
  private final DexWrapper dexWrapper;

  public DexRegularPathQueries(DexWrapper dexWrapper, long maxNodeId) {
    super(maxNodeId);
    this.dexWrapper = dexWrapper;
  }

  @Override
  protected void calculateRegularPaths(int aNodeName) throws FlockException, IOException {
    long aNodeId = dexWrapper.getNodeId(aNodeName);

    Graph graph = dexWrapper.getGraph();

    try(Objects cNodeCandidateIds = graph.neighbors(aNodeId, dexWrapper.getEdgeTypeL3(), Ingoing)) {
      for (long cNodeCandidateId : cNodeCandidateIds) {
        try(Objects bNodeCandidateIds = graph.neighbors(cNodeCandidateId, dexWrapper.getEdgeTypeL2(), Ingoing)) {
          for (long bNodeCandidateId : bNodeCandidateIds) {
            long edge = graph.findEdge(dexWrapper.getEdgeTypeL1(), aNodeId, bNodeCandidateId);
            if (edge != Objects.InvalidOID) {
              printHit(
                aNodeName,
                dexWrapper.getNodeName(bNodeCandidateId),
                dexWrapper.getNodeName(cNodeCandidateId));
            }
          }
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    try(DexWrapper dexWrapper = new DexWrapper("benchmark.dex")) {
      new DexRegularPathQueries(dexWrapper, 511).calculateRegularPaths();
    }
  }
}
