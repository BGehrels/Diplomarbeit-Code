package info.gehrels.diplomarbeit.dex;

import com.sparsity.dex.algorithms.TraversalBFS;
import com.sparsity.dex.gdb.EdgesDirection;
import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.diplomarbeit.AbstractFriendsOfFriends;
import java.io.IOException;


public class DexFriendsOfFriends extends AbstractFriendsOfFriends {
  private final DexWrapper dexWrapper;

  public DexFriendsOfFriends(DexWrapper dexWrapper, long maxNodeId) {
    super(maxNodeId);
    this.dexWrapper = dexWrapper;
  }

  @Override
  protected void calculateFriendsOfFriends(long startNodeName) throws IOException, FlockException {
    try(TraversalBFS traversalBFS = new TraversalBFS(dexWrapper.getSession(), dexWrapper.getNodeId(startNodeName))) {
      traversalBFS.setMaximumHops(3);
      traversalBFS.addAllEdgeTypes(EdgesDirection.Outgoing);
      traversalBFS.addAllNodeTypes();

      while (traversalBFS.hasNext()) {
        long next = traversalBFS.next();
        printFriendNode(startNodeName, dexWrapper.getNodeName(next));
      }

    }
  }

  public static void main(String[] args) throws Exception {
    try(DexWrapper dexWrapper = new DexWrapper("benchmark.dex")) {
      new DexFriendsOfFriends(dexWrapper, 511).calculateFriendsOfFriends();
    }
  }
}
