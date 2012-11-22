package info.gehrels.diplomarbeit.neo4j;

import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.flockDBClient.FlockDB;
import info.gehrels.flockDBClient.SelectionQuery;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.lang.Integer.parseInt;

public class FlockDBRegularPathQuery extends AbstractRegularPathQuery<FlockDB>{
	public static void main(String[] args) throws Exception {
		new FlockDBRegularPathQuery(parseInt(args[0])).calculateRegularPaths();
	}
	public FlockDBRegularPathQuery(int maxNodeId) throws IOException {
		super(maxNodeId, FlockDBHelper.createFlockDB());
	}

	@Override
	protected void calculateRegularPaths(int aNode) throws FlockException, IOException {
		SortedSet<Triplet<Long>> results = new TreeSet<>();

		NonPagedResultList cResultList = new NonPagedResultList(
			graphDB.select(SelectionQuery.simpleSelection(aNode, 3, false)).execute().get(0));

		// TODO: Mehrere Anfragen gleichzeitig?
		// TODO: Union Query?
		for (Long cNode : cResultList) {
			NonPagedResultList bResultList = new NonPagedResultList(
				graphDB.select(SelectionQuery.simpleSelection(cNode, 2, false)).execute().get(0));
			for (Long bNode : bResultList) {
				NonPagedResultList aResultList = new NonPagedResultList(
					graphDB.select(SelectionQuery.simpleSelection(bNode, 1, false, aNode)).execute().get(0));
				if (aResultList.iterator().hasNext()) {
					results.add(new Triplet<>((long) aNode, bNode, cNode));
				}
			}
		}

		for (Triplet<Long> result : results) {
			System.out.println(result);
		}
	}

	private static class Triplet<T extends Comparable<T>> implements Comparable<Triplet<T>>{

		public final T elem1;
		public final T elem2;
		public final T elem3;

		public Triplet(T elem1, T elem2, T elem3) {
			this.elem1 = elem1;
			this.elem2 = elem2;
			this.elem3 = elem3;
		}

		@Override
		public int compareTo(Triplet<T> o) {
			int result = elem1.compareTo(o.elem1);
			if (result == 0) {
				result = elem2.compareTo(o.elem2);
			}

			if (result == 0) {
				result = elem3.compareTo(o.elem3);
			}
			return result;
		}

		@Override
		public String toString() {
			return elem1 + ", " + elem2 + ", " + elem3;
		}
	}
}
