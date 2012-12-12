package info.gehrels.diplomarbeit.hypergraphdb;

import info.gehrels.diplomarbeit.AbstractFriendsOfFriends;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.algorithms.DefaultALGenerator;
import org.hypergraphdb.algorithms.HGBreadthFirstTraversal;
import org.hypergraphdb.algorithms.HGTraversal;
import org.hypergraphdb.query.AnyAtomCondition;
import org.hypergraphdb.util.Pair;

public class HyperGraphDBFriendsOfFriends extends AbstractFriendsOfFriends {
	private final HyperGraph hyperGraph;

	public static void main(String[] args) throws Exception {
		new HyperGraphDBFriendsOfFriends(HyperGraphDBHelper.createHyperGraphDB(args[0]), Long.parseLong(args[1])).calculateFriendsOfFriends();
	}

	public HyperGraphDBFriendsOfFriends(HyperGraph hyperGraph, long maxNodeId) {
		super(maxNodeId);
		this.hyperGraph = hyperGraph;
	}

	@Override
	protected void calculateFriendsOfFriends(long startNodeId) {
		HGHandle startAtom = (HGHandle) hyperGraph.findOne(hg.eq(startNodeId));
		HGTraversal traversal = new HGBreadthFirstTraversal(startAtom,
		                                                            new DefaultALGenerator(hyperGraph,
		                                                                                   new AnyAtomCondition(),
		                                                                                   new AnyAtomCondition(),
		                                                                                   false, true, false, true), 3);

		while (traversal.hasNext()) {
			Pair<HGHandle, HGHandle> next = traversal.next();
			printFriendNode(startNodeId, (Long) hyperGraph.get(next.getSecond()));
		}
	}
}
