package info.gehrels.diplomarbeit.flockdb;

import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.diplomarbeit.AbstractRegularPathQuery;
import info.gehrels.flockDBClient.FlockDB;

import java.io.IOException;

import static info.gehrels.diplomarbeit.flockdb.FlockDBHelper.createFlockDB;
import static info.gehrels.flockDBClient.Direction.INCOMING;
import static info.gehrels.flockDBClient.SelectionQuery.simpleSelection;
import static java.lang.Integer.parseInt;

public class FlockDBRegularPathQuery extends AbstractRegularPathQuery {
	private final FlockDB graphDB;

	public static void main(String[] args) throws Exception {
		new FlockDBRegularPathQuery(createFlockDB(), parseInt(args[0])).calculateRegularPaths();
	}

	public FlockDBRegularPathQuery(FlockDB flockDB, long maxNodeId) throws IOException {
		super(maxNodeId);
		graphDB = flockDB;
	}

	@Override
	protected void calculateRegularPaths(int aNode) throws FlockException, IOException {
		NonPagedResultList cResultList = new NonPagedResultList(
			graphDB.select(simpleSelection(aNode, 3, INCOMING)).execute().get(0)
		);

		// TODO: Mehrere Anfragen gleichzeitig?
		// TODO: Union Query?
		for (Long cNode : cResultList) {
			NonPagedResultList bResultList = new NonPagedResultList(
				graphDB.select(simpleSelection(cNode, 2, INCOMING)).execute().get(0));
			for (Long bNode : bResultList) {
				NonPagedResultList aResultList = new NonPagedResultList(
					graphDB.select(simpleSelection(bNode, 1, INCOMING, aNode)).execute().get(0));
				if (aResultList.iterator().hasNext()) {
					printHit(aNode, bNode, cNode);
				}
			}
		}
	}

}
