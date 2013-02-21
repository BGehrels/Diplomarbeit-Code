package info.gehrels.diplomarbeit.dex;

import com.sparsity.dex.gdb.Graph;
import com.sparsity.dex.gdb.Objects;
import info.gehrels.diplomarbeit.AbstractCommonFriends;
import static com.sparsity.dex.gdb.EdgesDirection.Outgoing;
import static com.sparsity.dex.gdb.Objects.combineIntersection;


public class DexCommonFriends extends AbstractCommonFriends {
  private final DexWrapper dexWrapper;

  public DexCommonFriends(DexWrapper dexWrapper, long maxNodeId) {
    super(maxNodeId);

    this.dexWrapper = dexWrapper;
  }

  @Override
  protected void calculateCommonFriends(int nodeName1, int nodeName2) throws Exception {
    long firstNodeId = dexWrapper.getNodeId(nodeName1);
    long secondNodeId = dexWrapper.getNodeId(nodeName2);

    Graph graph = dexWrapper.getGraph();


    try(Objects firstNodesNeighbours = graph.neighbors(firstNodeId, dexWrapper.getEdgeTypeL1(), Outgoing);
      Objects secondNodexNeighbours = graph.neighbors(secondNodeId, dexWrapper.getEdgeTypeL1(), Outgoing);
      Objects commonFriends = combineIntersection(firstNodesNeighbours, secondNodexNeighbours)) {
      for (Long commonFriendId : commonFriends) {
        printCommonFriend(nodeName1, nodeName2, dexWrapper.getNodeName(commonFriendId));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    try(DexWrapper dexWrapper = new DexWrapper("benchmark.dex")) {
      new DexCommonFriends(dexWrapper, 511).calculateCommonFriends();
    }
  }
}
