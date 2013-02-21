package info.gehrels.diplomarbeit.dex;

import com.sparsity.dex.algorithms.ConnectedComponents;
import com.sparsity.dex.algorithms.StrongConnectivity;
import com.sparsity.dex.algorithms.StrongConnectivityGabow;
import com.sparsity.dex.gdb.EdgesDirection;
import com.sparsity.dex.gdb.Objects;
import java.io.FileNotFoundException;
import java.util.SortedSet;
import java.util.TreeSet;


public class DexStronglyConnectedComponentsCalculator {
  private final DexWrapper dexWrapper;

  public DexStronglyConnectedComponentsCalculator(DexWrapper dexWrapper) {
    this.dexWrapper = dexWrapper;
  }

  public void calculateStronglyConnectedComponents() {
    try(StrongConnectivity strongConnectivity = new StrongConnectivityGabow(dexWrapper.getSession())) {
      strongConnectivity.addAllEdgeTypes(EdgesDirection.Outgoing);
      strongConnectivity.addAllNodeTypes();
      strongConnectivity.run();

      try(ConnectedComponents connectedComponents = strongConnectivity.getConnectedComponents()) {
        for (long i = 0; i < connectedComponents.getCount(); i++) {
          SortedSet<Long> sortedSCCNodeNames = new TreeSet<>();
          try(Objects nodes = connectedComponents.getNodes(i)) {
            for (long node : nodes) {
              sortedSCCNodeNames.add(dexWrapper.getNodeName(node));
            }
          }

          System.out.println("SCC: " + sortedSCCNodeNames);

        }
      }
    }
  }

  public static void main(String[] args) throws FileNotFoundException {
    try(DexWrapper wrapper = new DexWrapper("benchmark.dex")) {
      new DexStronglyConnectedComponentsCalculator(wrapper).calculateStronglyConnectedComponents();
    }
  }
}
