package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.flockDBClient.FlockDB;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import static info.gehrels.diplomarbeit.neo4j.FlockDBHelper.getAllOutgoingRelationshipsFor;

public class FlockDBFriendsOfFriends extends AbstractFriendsOfFriends {
	private final FlockDB graphDb;

	public FlockDBFriendsOfFriends(long maxNodeId) throws IOException {
		super(maxNodeId);
		graphDb = FlockDBHelper.createFlockDB();
	}

	public static void main(String... args) throws IOException, FlockException {
		Stopwatch stopwatch = new Stopwatch().start();
		new FlockDBFriendsOfFriends(Long.parseLong(args[0])).calculateFriendsOfFriends();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	@Override
	protected void calculateFriendsOfFriends(long startNodeId) throws IOException, FlockException {
		Set<Long> alreadyTraversed = new HashSet<>();

		Queue<Long> currentDepthNodeQueue = new LinkedList<>();
		Queue<Long> nextDepthLevelNodeQueue = new LinkedList<>();
		long level = 1;
		currentDepthNodeQueue.add(startNodeId);

		while (level <= 3) {
			Long nodeId;
			while ((nodeId = currentDepthNodeQueue.poll()) != null) {
				alreadyTraversed.add(nodeId);
				for (Long friend : getAllOutgoingRelationshipsFor(graphDb, nodeId)) {
					nextDepthLevelNodeQueue.add(friend);
				}
			}

			nextDepthLevelNodeQueue.removeAll(alreadyTraversed);
			currentDepthNodeQueue = nextDepthLevelNodeQueue;
			nextDepthLevelNodeQueue = new LinkedList<>();
			level++;
		}
	}

}
