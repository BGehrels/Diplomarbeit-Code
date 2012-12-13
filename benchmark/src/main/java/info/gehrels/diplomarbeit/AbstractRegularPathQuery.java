package info.gehrels.diplomarbeit;

import com.twitter.flockdb.thrift.FlockException;

import java.io.IOException;

public abstract class AbstractRegularPathQuery {
	protected final long maxNodeId;

	public AbstractRegularPathQuery(long maxNodeId) {
		this.maxNodeId = maxNodeId;
	}

	public void calculateRegularPaths() throws Exception {
		for (Integer nodeId : new RandomNodeIdGenerator(maxNodeId, 1000)) {
			calculateRegularPaths(nodeId);
		}
	}

	protected abstract void calculateRegularPaths(int id1) throws FlockException, IOException;

	protected void printHit(long a, long b, long c) {
		System.out.println(a + ", " + b + ", " + c);
	}
}
