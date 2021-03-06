package info.gehrels.diplomarbeit.hypergraphdb;

import info.gehrels.diplomarbeit.AbstractCommonFriends;
import info.gehrels.diplomarbeit.Measurement;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import java.util.HashSet;
import java.util.Set;
import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.getFriendNodes;
import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.createGetNodeByIdQuery;
import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.createHyperGraphDB;
import static java.lang.Long.parseLong;


public class HyperGraphCommonFriends extends AbstractCommonFriends {
  private final HyperGraph database;
  private final HGQuery<HGHandle> findNodeById;

  public static void main(final String[] args) throws Exception {
    measure(new Measurement<Void>() {
        @Override
        public void execute(Void database) throws Exception {
          new HyperGraphCommonFriends(createHyperGraphDB(args[0], true), parseLong(args[1])).calculateCommonFriends();
        }
      });
  }

  public HyperGraphCommonFriends(HyperGraph database, long maxNodeId) {
    super(maxNodeId);
    this.database = database;
    this.findNodeById = createGetNodeByIdQuery(database);

  }

  @Override
  protected void calculateCommonFriends(int id1, int id2) throws Exception {
    Set<HGHandle> friendsOf1 = getFriendsOf(id1);
    Set<HGHandle> friendsOf2 = getFriendsOf(id2);

    friendsOf1.retainAll(friendsOf2);

    for (HGHandle hgHandle : friendsOf1) {
      printCommonFriend(id1, id2, (Long) database.get(hgHandle));
    }
  }

  private Set<HGHandle> getFriendsOf(long id1) {
    HGHandle node1 = findNodeById.var("id", id1).findOne();
    HGSearchResult<HGHandle> l1 = getFriendNodes(database, "L1", node1, true);
    Set<HGHandle> friendNodesOfNode1 = new HashSet<>();
    while (l1.hasNext()) {
      friendNodesOfNode1.add(l1.next());
    }
    l1.close();
    return friendNodesOfNode1;
  }
}
