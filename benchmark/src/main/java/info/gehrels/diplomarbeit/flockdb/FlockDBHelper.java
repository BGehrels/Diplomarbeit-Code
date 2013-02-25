package info.gehrels.diplomarbeit.flockdb;

import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.flockDBClient.FlockDB;
import info.gehrels.flockDBClient.PagedNodeIdList;
import info.gehrels.flockDBClient.SelectionQuery;
import java.io.IOException;
import java.util.Set;
import static info.gehrels.flockDBClient.Direction.OUTGOING;
import static info.gehrels.flockDBClient.SelectionQuery.simpleSelection;
import static info.gehrels.flockDBClient.SelectionQuery.union;
import static java.lang.Runtime.getRuntime;
import static java.util.Collections.emptyList;


public class FlockDBHelper {
  static Iterable<Long> getAllOutgoingRelationshipsFor(FlockDB graphDb, long nodeId) throws IOException,
                                                                                            FlockException {
    PagedNodeIdList result = graphDb.select(
      getQueryForAllOutgoingRelationshipsFor(nodeId)).execute().get(0);

    return new NonPagedResultList(result);
  }

  static Iterable<Long> getAllOutgoingRelationshipsFor(FlockDB graphDb, Set<Long> nodes) {
    SelectionQuery query = null;
    for (Long node : nodes) {
      if (query == null) {
        query = getQueryForAllOutgoingRelationshipsFor(node);
      } else {
        query = union(query, getQueryForAllOutgoingRelationshipsFor(node));
      }

    }

    if (query == null) {
      return emptyList();
    }

    PagedNodeIdList result = null;
    try {
      result = graphDb.select(query).execute().get(0);
    } catch (Exception e) {
      System.out.println(e);
      System.out.println(e.getMessage());
      System.out.println(nodes);
    }

    return new NonPagedResultList(result);
  }


  private static SelectionQuery getQueryForAllOutgoingRelationshipsFor(long nodeId) {
    return union(
      union(
        union(
          simpleSelection(nodeId, 1, OUTGOING),
          simpleSelection(nodeId, 2, OUTGOING)),
        simpleSelection(nodeId, 3, OUTGOING)),
      simpleSelection(nodeId, 4, OUTGOING));
  }

  static FlockDB createFlockDB() throws IOException {
    FlockDB graphDb = new FlockDB("localhost", 7915, 1000000);
    registerShutdownHook(graphDb);
    return graphDb;
  }

  private static void registerShutdownHook(final FlockDB graphDb) {
    getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          graphDb.close();
        }
      });
  }

}
