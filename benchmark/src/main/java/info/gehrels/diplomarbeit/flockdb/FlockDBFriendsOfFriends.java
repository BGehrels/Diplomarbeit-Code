package info.gehrels.diplomarbeit.flockdb;

import com.google.common.base.Stopwatch;
import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.diplomarbeit.AbstractFriendsOfFriends;
import info.gehrels.flockDBClient.FlockDB;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class FlockDBFriendsOfFriends extends AbstractFriendsOfFriends {
	private final FlockDB graphDb;
	private Set<Long> alreadyTraversed;

	public FlockDBFriendsOfFriends(FlockDB flockDB, long maxNodeId) throws IOException {
		super(maxNodeId);

		this.graphDb = flockDB;
	}

	public static void main(String... args) throws IOException, FlockException {
		Stopwatch stopwatch = new Stopwatch().start();
		new FlockDBFriendsOfFriends(FlockDBHelper.createFlockDB(), Long.parseLong(args[0])).calculateFriendsOfFriends();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	@Override
	protected void calculateFriendsOfFriends(long startNodeId) throws IOException, FlockException {
		alreadyTraversed = new HashSet<>();
		Set<Long> currentDepthNodes = new HashSet<>();
		currentDepthNodes.add(startNodeId);

		calculateFriendsOfFriends(startNodeId, currentDepthNodes, 0);
	}

	protected void calculateFriendsOfFriends(long startNodeId, Set<Long> currentDepthNodes, int currentDepth) throws
		FlockException, IOException {
		if (currentDepth != 0) {
			printFriendNodes(startNodeId, currentDepthNodes);
		}
		if (currentDepth > 3) {
			return;
		}

		Set<Long> nextDepthLevelNodes = new HashSet<>();
		for (Long nodeId : currentDepthNodes) {
			alreadyTraversed.add(nodeId);
			for (Long friend : FlockDBHelper.getAllOutgoingRelationshipsFor(graphDb, nodeId)) {
				nextDepthLevelNodes.add(friend);
			}
		}

		nextDepthLevelNodes.removeAll(alreadyTraversed);
		calculateFriendsOfFriends(startNodeId, nextDepthLevelNodes, currentDepth + 1);
	}

	private void printFriendNodes(long startNodeId, Iterable<Long> nextDepthLevelNodes) {
		for (Long aLong : nextDepthLevelNodes) {
			printFriendNode(startNodeId, aLong);
		}
	}

}
