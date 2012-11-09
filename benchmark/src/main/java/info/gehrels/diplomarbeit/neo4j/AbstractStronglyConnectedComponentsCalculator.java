package info.gehrels.diplomarbeit.neo4j;

import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;

public class AbstractStronglyConnectedComponentsCalculator {
	protected Stack<Long> sccCandidatesStack;

	protected void printOutSCC(long nodeName) {
		SortedSet<Long> sortedNodeIds = new TreeSet<>();
		Long pop;
		do {
			pop = sccCandidatesStack.pop();
			sortedNodeIds.add(pop);
		} while (!pop.equals(nodeName));

		System.out.println("SCC: " + sortedNodeIds);
	}
}
