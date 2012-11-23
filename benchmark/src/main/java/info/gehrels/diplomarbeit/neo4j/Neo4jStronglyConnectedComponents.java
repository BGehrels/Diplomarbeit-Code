package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Neo4jStronglyConnectedComponents extends AbstractStronglyConnectedComponentsCalculator<GraphDatabaseService> {
	private Set<Long> alreadyVisitedNodes;
	private long depthFirstVisitIndex;
	private Map<Long, Long> nodeToDfbiMap;

	public static void main(String... args) {
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jStronglyConnectedComponents(args[0]).calculateStronglyConnectedComponents();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	public Neo4jStronglyConnectedComponents(String dbPath) {
		super(Neo4jHelper.createNeo4jDatabase(dbPath));
	}

	private Neo4jStronglyConnectedComponents calculateStronglyConnectedComponents() {
		alreadyVisitedNodes = new HashSet<>();
		depthFirstVisitIndex = 0;
		nodeToDfbiMap = new HashMap<>();

		for (Node node : GlobalGraphOperations.at(graphDB).getAllNodes()) {
			if (node.getId() == 0)
				continue;
			Long nodeName = (Long) node.getProperty(Neo4jImporter.NAME_KEY);
			if (!alreadyVisitedNodes.contains(nodeName)) {
				calculateStronglyConnectedComponentsDepthFirst(node);
			}
		}

		return this;
	}

	private long calculateStronglyConnectedComponentsDepthFirst(Node node) {
		Long nodeName = (Long) node.getProperty(Neo4jImporter.NAME_KEY);
		alreadyVisitedNodes.add(nodeName);
		depthFirstVisitIndex++;
		nodeToDfbiMap.put(nodeName, depthFirstVisitIndex);
		long mySccRoot = depthFirstVisitIndex;
		sccCandidatesStack.push(nodeName);

		for (Relationship relationship : node.getRelationships(Direction.OUTGOING)) {
			Node endNode = relationship.getEndNode();
			Long endNodeName = (Long) endNode.getProperty(Neo4jImporter.NAME_KEY);
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
			printOutSCC(nodeName);
		}

		return mySccRoot;

	}
}
