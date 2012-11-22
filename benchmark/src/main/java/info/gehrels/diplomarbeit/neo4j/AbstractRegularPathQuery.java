package info.gehrels.diplomarbeit.neo4j;

import com.twitter.flockdb.thrift.FlockException;

import java.io.IOException;

public abstract class AbstractRegularPathQuery<DB> {
	protected final DB graphDB;
	protected final int maxNodeId;

	public AbstractRegularPathQuery(int maxNodeId, DB db) {
		graphDB = db;
		this.maxNodeId = maxNodeId;
	}

	protected void calculateRegularPaths() throws Exception {
		for (Integer nodeId : new RandomNodeIdGenerator(maxNodeId, 1000)) {
			calculateRegularPaths(nodeId);
		}
	}

	protected abstract void calculateRegularPaths(int id1) throws FlockException, IOException;
}
