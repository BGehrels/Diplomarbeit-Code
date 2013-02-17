package info.gehrels.diplomarbeit.hypergraphdb;

import org.hypergraphdb.*;

import java.io.IOException;

import static org.hypergraphdb.HGQuery.hg.*;


public class HyperGraphDBHelper {
  static HyperGraph createHyperGraphDB(String dbPath) throws IOException {
    HGConfiguration config = new HGConfiguration();
    config.setTransactional(false);
    config.setUseSystemAtomAttributes(false);
    return HGEnvironment.get(dbPath, config);
  }

  static HGQuery<HGHandle> createGetNodeByIdQuery(HyperGraph database) {
    return make(HGHandle.class, database).compile(eq(var("id")));
  }

  static HGSearchResult<HGHandle> getFriendNodes(HyperGraph database, String edgeLabel, HGHandle handle, boolean outgoing) {
    return database.find(
      apply(
        targetAt(database, outgoing ? 1 : 0),
        and(
          eq(edgeLabel),
          outgoing ? orderedLink(handle, anyHandle()) : orderedLink(anyHandle(), handle)
        )
      )
    );
  }
}
