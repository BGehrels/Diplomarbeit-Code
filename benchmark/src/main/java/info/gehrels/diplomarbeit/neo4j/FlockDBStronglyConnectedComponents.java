package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.flockDBClient.FlockDB;
import info.gehrels.flockDBClient.PagedNodeIdList;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import static info.gehrels.flockDBClient.SelectionQuery.simpleSelection;
import static info.gehrels.flockDBClient.SelectionQuery.union;

public class FlockDBStronglyConnectedComponents extends AbstractStronglyConnectedComponentsCalculator {
	private final FlockDB graphDb;
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
		graphDb = new FlockDB("localhost", 7915, 1000000);
		registerShutdownHook(graphDb);

		this.maxNodeId = maxNodeId;
	}

	private FlockDBStronglyConnectedComponents calculateStronglyConnectedComponents() throws IOException,
		FlockException {
		alreadyVisitedNodes = new HashSet<>();
		depthFirstVisitIndex = 0;
		nodeToDfbiMap = new HashMap<>();
		sccCandidatesStack = new Stack<>();

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

		for (Long endNode : getAllOutgoingRelationshipsFor(nodeId)) {
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

	private Iterable<Long> getAllOutgoingRelationshipsFor(long nodeId) throws IOException, FlockException {
		final PagedNodeIdList result = graphDb.select(
			union(
				union(
					union(
						simpleSelection(nodeId, 1, true),
						simpleSelection(nodeId, 2, true)
					),
					simpleSelection(nodeId, 3, true)
				),
				simpleSelection(nodeId, 4, true)
			)
		).execute().get(0);

		return new Iterable<Long>() {
			@Override
			public Iterator<Long> iterator() {
				return new Iterator<Long>() {
					PagedNodeIdList currentResultPage = result;
					Iterator<Long> myIterator = result.iterator();
					@Override
					public boolean hasNext() {
						if (myIterator.hasNext()) {
							return true;
						}

						if (currentResultPage.hasNextPage()) {
							try {
								currentResultPage = currentResultPage.getNextPage();
								myIterator = currentResultPage.iterator();
							} catch (Exception e) {
								throw new IllegalStateException(e);
							}
						}

						return myIterator.hasNext();
					}

					@Override
					public Long next() {
						return myIterator.next();
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}
		};
	}

	private static void registerShutdownHook(final FlockDB graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.close();
			}
		});
	}
}
