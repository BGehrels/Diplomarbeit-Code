package info.gehrels.diplomarbeit;

import com.twitter.flockdb.thrift.FlockException;

import java.io.IOException;

public abstract class AbstractFriendsOfFriends {
	protected final long maxNodeId;

	public AbstractFriendsOfFriends(long maxNodeId) {
		this.maxNodeId = maxNodeId;
	}

	public void calculateFriendsOfFriends() throws FlockException, IOException {
		for (int nodeId : new RandomNodeIdGenerator(maxNodeId, 1000)) {
			calculateFriendsOfFriends(nodeId);
		}
	}

	protected abstract void calculateFriendsOfFriends(long startNodeId) throws IOException, FlockException;

	protected void printFriendNode(long startNodeId, long traversedNodeId) {
		System.out.println(startNodeId + ": " + traversedNodeId);
	}
}
