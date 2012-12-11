package info.gehrels.diplomarbeit.hypergraphdb;

import info.gehrels.diplomarbeit.AbstractStronglyConnectedComponentsCalculator;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;

public class HyperGraphDBStronglyConnectedComponents extends AbstractStronglyConnectedComponentsCalculator<HyperGraph, HGHandle> {
	public HyperGraphDBStronglyConnectedComponents(HyperGraph hyperGraph) {
		super(hyperGraph);
	}

	public void calculateStronglyConnectedComponents() {
		throw new UnsupportedOperationException();
	}

	@Override
	protected Iterable<HGHandle> getAllNodes() {
		throw new UnsupportedOperationException();
	}

	@Override
	protected long getNodeName(HGHandle node) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected Iterable<HGHandle> getOutgoingIncidentNodes(HGHandle node) throws Exception {
		throw new UnsupportedOperationException();
	}
}
