package info.gehrels.diplomarbeit;

import com.twitter.flockdb.thrift.FlockException;

import java.io.IOException;

public abstract class AbstractRegularPathQuery<DB> {
	protected final DB graphDB;
	protected final long maxNodeId;

	public AbstractRegularPathQuery(DB db, long maxNodeId) {
		graphDB = db;
		this.maxNodeId = maxNodeId;
	}

	public void calculateRegularPaths() throws Exception {
		for (Integer nodeId : new RandomNodeIdGenerator(maxNodeId, 1000)) {
			calculateRegularPaths(nodeId);
		}
	}

	protected abstract void calculateRegularPaths(int id1) throws FlockException, IOException;
}
