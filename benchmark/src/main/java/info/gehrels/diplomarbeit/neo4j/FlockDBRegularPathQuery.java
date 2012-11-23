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
		SortedSet<Triplet<Long, Long, Long>> results = new TreeSet<>();

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

		for (Triplet<Long, Long, Long> result : results) {
			System.out.println(result);
		}
	}

}
