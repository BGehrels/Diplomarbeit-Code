package info.gehrels.diplomarbeit.hypergraphdb;

import info.gehrels.diplomarbeit.AbstractFriendsOfFriends;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.algorithms.DefaultALGenerator;
import org.hypergraphdb.algorithms.HGBreadthFirstTraversal;
import org.hypergraphdb.algorithms.HGTraversal;
import org.hypergraphdb.query.AnyAtomCondition;
import org.hypergraphdb.util.Pair;

import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.createGetNodeByIdQuery;
import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.createHyperGraphDB;
import static java.lang.Long.parseLong;

public class HyperGraphDBFriendsOfFriends extends AbstractFriendsOfFriends {
	private final HyperGraph hyperGraph;
	private final HGQuery<HGHandle> queryForNodeById;

	public static void main(String[] args) throws Exception {
		new HyperGraphDBFriendsOfFriends(createHyperGraphDB(args[0]), parseLong(args[1])).calculateFriendsOfFriends();
	}

	public HyperGraphDBFriendsOfFriends(HyperGraph hyperGraph, long maxNodeId) {
		super(maxNodeId);
		this.hyperGraph = hyperGraph;
		this.queryForNodeById = createGetNodeByIdQuery(hyperGraph);
	}

	@Override
	protected void calculateFriendsOfFriends(long startNodeId) {
		HGHandle startAtom = queryForNodeById.var("id", startNodeId).findOne();
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
