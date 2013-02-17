package info.gehrels.diplomarbeit.hypergraphdb;

import info.gehrels.diplomarbeit.AbstractStronglyConnectedComponentsCalculator;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;

import java.util.Iterator;

import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.createHyperGraphDB;
import static org.hypergraphdb.HGQuery.hg.apply;
import static org.hypergraphdb.HGQuery.hg.findAll;
import static org.hypergraphdb.HGQuery.hg.incidentAt;
import static org.hypergraphdb.HGQuery.hg.targetAt;
import static org.hypergraphdb.HGQuery.hg.type;


public class HyperGraphDBStronglyConnectedComponents extends AbstractStronglyConnectedComponentsCalculator<HGHandle> {
  protected final HyperGraph graphDB;

  public static void main(String[] args) throws Exception {
    new HyperGraphDBStronglyConnectedComponents(createHyperGraphDB(args[0], true)).calculateStronglyConnectedComponents();
  }

  public HyperGraphDBStronglyConnectedComponents(HyperGraph hyperGraph) {
    this.graphDB = hyperGraph;
  }

  @Override
  protected Iterable<HGHandle> getAllNodes() {
    return findAll(graphDB, type(Long.class));
  }

  @Override
  protected long getNodeName(HGHandle node) {
    return graphDB.get(node);
  }

  @Override
  protected Iterable<HGHandle> getOutgoingIncidentNodes(final HGHandle node) throws Exception {
    return new Iterable<HGHandle>() {
      @Override
      public Iterator<HGHandle> iterator() {
        return graphDB.find(
          apply(
            targetAt(graphDB, 1),
            incidentAt(node, 0)));
      }
    };
  }
}
