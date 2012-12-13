package info.gehrels.diplomarbeit.hypergraphdb;

import info.gehrels.diplomarbeit.AbstractCommonFriends;
import info.gehrels.diplomarbeit.Measurement;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HyperGraphCommonFriends extends AbstractCommonFriends {
	private final HyperGraph database;

	public static void main(final String[] args) throws Exception {
		Measurement.measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				new HyperGraphCommonFriends(HyperGraphDBHelper.createHyperGraphDB(args[0]), Long.parseLong(args[1])).calculateCommonFriends();
			}
		});
	}

	public HyperGraphCommonFriends(HyperGraph database, long maxNodeId) {
		super(maxNodeId);
		this.database = database;
	}

	@Override
	protected void calculateCommonFriends(int id1, int id2) throws Exception {
		Set<HGHandle> friendsOf1 = getFriendsOf(id1);
		Set<HGHandle> friendsOf2 = getFriendsOf(id2);

		friendsOf1.retainAll(friendsOf2);

		for (HGHandle hgHandle : friendsOf1) {
			printCommonFriend(id1, id2, (Long) database.get(hgHandle));
		}
	}

	private Set<HGHandle> getFriendsOf(long id1) {
		HGHandle node1 = database.findOne(hg.eq(id1));
		List<HGValueLink> l1 = database.getAll(hg.and(hg.eq("L1"), hg.incidentAt(node1, 0)));
		Set<HGHandle> friendNodesOfNode1 = new HashSet<>();
		for (HGValueLink hgHandle : l1) {
			HGHandle friendNode = hgHandle.getTargetAt(1);
			friendNodesOfNode1.add(friendNode);
		}

		return friendNodesOfNode1;
	}
}
