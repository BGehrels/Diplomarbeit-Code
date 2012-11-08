package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class Neo4jStronglyConnectedComponents {
	private final GraphDatabaseService graphDb;
	private Set<Long> alreadyVisitedNodes;
	private long depthFirstVisitIndex;
	private Map<Long, Long> nodeToDfbiMap;
	private Stack<Long> sccCandidatesStack;

	public static void main(String... args) {
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jStronglyConnectedComponents(args[0]).calculateStronglyConnectedComponents();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	public Neo4jStronglyConnectedComponents(String dbPath) {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		registerShutdownHook(graphDb);
	}

	private Neo4jStronglyConnectedComponents calculateStronglyConnectedComponents() {
		alreadyVisitedNodes = new HashSet<>();
		depthFirstVisitIndex = 0;
		nodeToDfbiMap = new HashMap<>();
		sccCandidatesStack = new Stack<>();

		for (Node node : GlobalGraphOperations.at(graphDb).getAllNodes()) {
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

	private void printOutSCC(Long nodeName) {
		StringBuilder sccResultString = new StringBuilder("SCC: ");
		Long pop;
		do {
			pop = sccCandidatesStack.pop();

			sccResultString.append(pop).append(",");
		} while (!pop.equals(nodeName));

		System.out.println(sccResultString);
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
}
