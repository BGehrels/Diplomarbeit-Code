package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.flockDBClient.FlockDB;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static info.gehrels.diplomarbeit.neo4j.FlockDBHelper.getAllOutgoingRelationshipsFor;

public class FlockDBStronglyConnectedComponents extends AbstractStronglyConnectedComponentsCalculator<FlockDB> {
	private Set<Long> alreadyVisitedNodes;
	private long depthFirstVisitIndex;
	private Map<Long, Long> nodeToDfbiMap;
	private final long maxNodeId;

	public static void main(String... args) throws IOException, FlockException {
		Stopwatch stopwatch = new Stopwatch().start();
		new FlockDBStronglyConnectedComponents(Long.parseLong(args[0])).calculateStronglyConnectedComponents();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	public FlockDBStronglyConnectedComponents(long maxNodeId) throws IOException {
		super(FlockDBHelper.createFlockDB());

		this.maxNodeId = maxNodeId;
	}

	private FlockDBStronglyConnectedComponents calculateStronglyConnectedComponents() throws IOException,
		FlockException {
		alreadyVisitedNodes = new HashSet<>();
		depthFirstVisitIndex = 0;
		nodeToDfbiMap = new HashMap<>();

		for (long nodeId = 0; nodeId <= maxNodeId; nodeId++) {
			if (!alreadyVisitedNodes.contains(nodeId)) {
				calculateStronglyConnectedComponentsDepthFirst(nodeId);
			}
		}

		return this;
	}

	private long calculateStronglyConnectedComponentsDepthFirst(long nodeId) throws FlockException, IOException {
		alreadyVisitedNodes.add(nodeId);
		depthFirstVisitIndex++;
		nodeToDfbiMap.put(nodeId, depthFirstVisitIndex);
		long mySccRoot = depthFirstVisitIndex;
		sccCandidatesStack.push(nodeId);

		for (Long endNode : getAllOutgoingRelationshipsFor(graphDB, nodeId)) {
			if (!alreadyVisitedNodes.contains(endNode)) {
				long endNodesSccRoot = calculateStronglyConnectedComponentsDepthFirst(endNode);
				// Wenn endNode.sccId < my.sccId, dann haben wir einen Rückwärstpfad zu einem Knoten gefunden,
				// der weiter hinten im Call-Stack liegt
				mySccRoot = Math.min(
					mySccRoot,
					endNodesSccRoot
				);
			} else if (sccCandidatesStack.contains(endNode)) {
				// Mit ein wenig glück führt uns diese Kante sogar noch weiter zurück in die eigene Geschichte als die
				// bisher gefundenen Rückwärtspfade
				mySccRoot = Math.min(
					mySccRoot,
					nodeToDfbiMap.get(endNode)
				);
			}
		}

		if (mySccRoot == nodeToDfbiMap.get(nodeId)) {
			// Wir haben den zuerst touchierten Knoten einer SCC gefunden
			printOutSCC(nodeId);
		}

		return mySccRoot;

	}
}
