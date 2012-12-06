package info.gehrels.diplomarbeit;

import java.util.Iterator;

public abstract class AbstractCommonFriends {
	protected final long maxNodeId;

	public AbstractCommonFriends(long maxNodeId) {
		this.maxNodeId = maxNodeId;
	}

	public void calculateCommonFriends() throws Exception {
		Iterator<Integer> id2Generator = new RandomNodeIdGenerator(maxNodeId, 1000).iterator();
		for (Integer id1 : new RandomNodeIdGenerator(maxNodeId, 1000)) {
			calculateCommonFriends(id1, id2Generator.next());
		}
	}

	protected abstract void calculateCommonFriends(int id1, int id2) throws Exception;

	protected final void printCommonFriend(long id1, long id2, long x) {
		System.out.println(id1 + "/" + id2 + ": " + x);
	}
}
