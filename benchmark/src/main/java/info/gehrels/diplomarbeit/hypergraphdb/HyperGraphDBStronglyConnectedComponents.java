package info.gehrels.diplomarbeit.hypergraphdb;

import info.gehrels.diplomarbeit.AbstractStronglyConnectedComponentsCalculator;
import info.gehrels.diplomarbeit.TransformingIteratorWrapper;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;

import java.util.List;

public class HyperGraphDBStronglyConnectedComponents
	extends AbstractStronglyConnectedComponentsCalculator<HyperGraph, HGHandle> {
	public static void main(String[] args) throws Exception {
		new HyperGraphDBStronglyConnectedComponents(
			HyperGraphDBHelper.createHyperGraphDB("C:/Users/Benjamin/Desktop/hypergraphdb"))
			.calculateStronglyConnectedComponents();
	}

	public HyperGraphDBStronglyConnectedComponents(HyperGraph hyperGraph) {
		super(hyperGraph);
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
	protected Iterable<HGHandle> getOutgoingIncidentNodes(HGHandle node) throws Exception {
		List<HGValueLink> all = graphDB.getAll(hg.incidentAt(node, 0));
		return new TransformingIteratorWrapper<HGValueLink, HGHandle>(all.iterator()) {
			@Override
			protected HGHandle calculateNext(HGValueLink next) {
				return next.getTargetAt(1);
			}
		};
	}
}
