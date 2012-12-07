package info.gehrels.diplomarbeit;

import java.util.HashMap;
import java.util.Map;

public abstract class CachingImporter<NODE_TYPE> extends AbstractImporter {
	private final Map<Long, NODE_TYPE> nodeCache = new HashMap<>(200000);

	public CachingImporter(String sourceFile) throws Exception {
		super(sourceFile);
	}

	@Override
	protected final void createNode(Node node) {
		nodeCache.put(node.id, createNodeForCache(node));
	}

	protected abstract NODE_TYPE createNodeForCache(Node node);

	@Override
	protected final void createEdge(Edge edge) throws Exception {
		NODE_TYPE from = nodeCache.get(edge.from);
		NODE_TYPE to = nodeCache.get(edge.to);
		createEdgeBetweenCachedNodes(from, to, edge.label);
	}

	protected abstract void createEdgeBetweenCachedNodes(NODE_TYPE from, NODE_TYPE to, String label) throws Exception;
}
