package info.gehrels.diplomarbeit.neo4j;

import com.twitter.flockdb.thrift.FlockException;

import java.io.IOException;

public abstract class AbstractFriendsOfFriends {
	protected final long maxNodeId;

	public AbstractFriendsOfFriends(long maxNodeId) {
		this.maxNodeId = maxNodeId;
	}

	protected void calculateFriendsOfFriends() throws FlockException, IOException {
		for (int nodeId : new RandomNodeIdGenerator(maxNodeId, 1000)) {
			calculateFriendsOfFriends(nodeId);
		}
	}

	protected abstract void calculateFriendsOfFriends(long startNodeId) throws IOException, FlockException;
}
