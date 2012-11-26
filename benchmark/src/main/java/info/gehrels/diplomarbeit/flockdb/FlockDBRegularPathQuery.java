package info.gehrels.diplomarbeit.flockdb;

import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.diplomarbeit.AbstractRegularPathQuery;
import info.gehrels.diplomarbeit.Triplet;
import info.gehrels.flockDBClient.FlockDB;
import info.gehrels.flockDBClient.SelectionQuery;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import static info.gehrels.flockDBClient.Direction.INCOMING;
import static java.lang.Integer.parseInt;

public class FlockDBRegularPathQuery extends AbstractRegularPathQuery<FlockDB> {
	public static void main(String[] args) throws Exception {
		new FlockDBRegularPathQuery(parseInt(args[0])).calculateRegularPaths();
	}
	public FlockDBRegularPathQuery(int maxNodeId) throws IOException {
		super(FlockDBHelper.createFlockDB(), maxNodeId);
	}

	@Override
	protected void calculateRegularPaths(int aNode) throws FlockException, IOException {
		SortedSet<Triplet<Long, Long, Long>> results = new TreeSet<>();

		NonPagedResultList cResultList = new NonPagedResultList(
			graphDB.select(SelectionQuery.simpleSelection(aNode, 3, INCOMING)).execute().get(0));

		// TODO: Mehrere Anfragen gleichzeitig?
		// TODO: Union Query?
		for (Long cNode : cResultList) {
			NonPagedResultList bResultList = new NonPagedResultList(
				graphDB.select(SelectionQuery.simpleSelection(cNode, 2, INCOMING)).execute().get(0));
			for (Long bNode : bResultList) {
				NonPagedResultList aResultList = new NonPagedResultList(
					graphDB.select(SelectionQuery.simpleSelection(bNode, 1, INCOMING, aNode)).execute().get(0));
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
