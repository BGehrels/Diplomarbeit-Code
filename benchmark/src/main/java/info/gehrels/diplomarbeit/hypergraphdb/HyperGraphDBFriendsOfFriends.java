package info.gehrels.diplomarbeit.hypergraphdb;

import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.diplomarbeit.AbstractFriendsOfFriends;
import org.hypergraphdb.HyperGraph;

import java.io.IOException;

public class HyperGraphDBFriendsOfFriends extends AbstractFriendsOfFriends {
	private final HyperGraph hyperGraph;

	public HyperGraphDBFriendsOfFriends(HyperGraph hyperGraph, long maxNodeId) {
		super(maxNodeId);
		this.hyperGraph = hyperGraph;
	}

	@Override
	protected void calculateFriendsOfFriends(long startNodeId) throws IOException, FlockException {
		throw new UnsupportedOperationException();
	}
}
