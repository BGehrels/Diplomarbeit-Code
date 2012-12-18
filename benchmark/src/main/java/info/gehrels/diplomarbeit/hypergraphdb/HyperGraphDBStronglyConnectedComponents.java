package info.gehrels.diplomarbeit.hypergraphdb;

import info.gehrels.diplomarbeit.AbstractStronglyConnectedComponentsCalculator;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;

import java.util.Iterator;

import static org.hypergraphdb.HGQuery.hg.apply;
import static org.hypergraphdb.HGQuery.hg.incidentAt;
import static org.hypergraphdb.HGQuery.hg.targetAt;
import static org.hypergraphdb.HGQuery.hg.var;

public class HyperGraphDBStronglyConnectedComponents
	extends AbstractStronglyConnectedComponentsCalculator<HGHandle> {
	protected final HyperGraph graphDB;
	private final HGQuery<HGHandle> outgoingIncidentNodesQuery;

	public static void main(String[] args) throws Exception {
		new HyperGraphDBStronglyConnectedComponents(
			HyperGraphDBHelper.createHyperGraphDB(args[0]))
			.calculateStronglyConnectedComponents();
	}

	public HyperGraphDBStronglyConnectedComponents(HyperGraph hyperGraph) {
		this.graphDB = hyperGraph;
		this.outgoingIncidentNodesQuery = hg.make(HGHandle.class, graphDB)
			.compile(
				apply(
					targetAt(graphDB, 1),
					incidentAt(
						var("sourceNode", HGHandle.class),
						0)
				)
			);
	}

	@Override
	protected Iterable<HGHandle> getAllNodes() {
		return hg.findAll(graphDB, hg.type(Long.class));
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
				return outgoingIncidentNodesQuery.var("sourceNode", node).execute();
			}
		};
	}
}
