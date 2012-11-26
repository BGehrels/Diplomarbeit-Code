package info.gehrels.diplomarbeit;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;

public abstract class AbstractStronglyConnectedComponentsCalculator<DB_TYPE, NODE_TYPE> {
	private Set<Long> alreadyVisitedNodes;
	private long depthFirstVisitIndex;
	private Map<Long, Long> nodeToDfbiMap;

	protected final Stack<Long> sccCandidatesStack = new Stack<>();
	protected final DB_TYPE graphDB;

	public AbstractStronglyConnectedComponentsCalculator(DB_TYPE graphDB) {
		this.graphDB = graphDB;
	}

	public void calculateStronglyConnectedComponents() throws Exception {
		alreadyVisitedNodes = new HashSet<>();
		depthFirstVisitIndex = 0;
		nodeToDfbiMap = new HashMap<>();

		for (NODE_TYPE node : getAllNodes()) {
			Long nodeName = getNodeName(node);
			if (!alreadyVisitedNodes.contains(nodeName)) {
				calculateStronglyConnectedComponentsDepthFirst(node);
			}
		}
	}

	private long calculateStronglyConnectedComponentsDepthFirst(NODE_TYPE node) throws Exception {
		long nodeName = getNodeName(node);
		alreadyVisitedNodes.add(nodeName);
		depthFirstVisitIndex++;
		nodeToDfbiMap.put(nodeName, depthFirstVisitIndex);
		long mySccRoot = depthFirstVisitIndex;
		sccCandidatesStack.push(nodeName);

		for (NODE_TYPE endNode : getOutgoingIncidentNodes(node)) {
			long endNodeName = getNodeName(endNode);
			if (!alreadyVisitedNodes.contains(endNodeName)) {
				long endNodesSccRoot = calculateStronglyConnectedComponentsDepthFirst(endNode);
				// Wenn endNode.sccId < my.sccId, dann haben wir einen Rückwärstpfad zu einem Knoten gefunden,
				// der weiter hinten im Call-Stack liegt
				mySccRoot = Math.min(
					mySccRoot,
					endNodesSccRoot
				);
			} else if (sccCandidatesStack.contains(endNodeName)) {
				// Mit ein wenig glück führt uns diese Kante sogar noch weiter zurück in die eigene Geschichte als die
				// bisher gefundenen Rückwärtspfade
				mySccRoot = Math.min(
					mySccRoot,
					nodeToDfbiMap.get(endNodeName)
				);
			}
		}

		if (mySccRoot == nodeToDfbiMap.get(nodeName)) {
			// Wir haben den zuerst touchierten Knoten einer SCC gefunden
			System.out.println(createResultSCCString(nodeName));
		}

		return mySccRoot;
	}

	protected String createResultSCCString(long nodeName) {
		SortedSet<Long> sortedNodeIds = new TreeSet<>();
		Long sccNodeIdFromStack;
		do {
			sccNodeIdFromStack = sccCandidatesStack.pop();
			sortedNodeIds.add(sccNodeIdFromStack);
		} while (!sccNodeIdFromStack.equals(nodeName));

		return "SCC: " + sortedNodeIds;
	}

	protected abstract Iterable<NODE_TYPE> getAllNodes();
	protected abstract long getNodeName(NODE_TYPE node);
	protected abstract Iterable<NODE_TYPE> getOutgoingIncidentNodes(NODE_TYPE node) throws Exception;

}
