package info.gehrels.diplomarbeit.hypergraphdb;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HyperGraph;

import java.io.IOException;

import static org.hypergraphdb.HGQuery.hg.and;
import static org.hypergraphdb.HGQuery.hg.apply;
import static org.hypergraphdb.HGQuery.hg.eq;
import static org.hypergraphdb.HGQuery.hg.incidentAt;
import static org.hypergraphdb.HGQuery.hg.make;
import static org.hypergraphdb.HGQuery.hg.targetAt;
import static org.hypergraphdb.HGQuery.hg.var;

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

	static HGQuery<HGHandle> createGetFriendNodesQuery(HyperGraph database, String edgeLabel, boolean outgoing) {
		// TODO: Mal ausprobieren, ob orderedLink(anyHandle(), var("node", HGHandle.class)) schneller ist, wenn es mit
		// TODO: einem TargetAt index kombiniert wird. Funzt aber nut, wenn keine var() benutzt wird, sondern "echtes"
		// TODO: handle. Eventuell könnte mensch auch einen CompositeIndex zusammen mit einer IndexCondition nutzen?
		return make(HGHandle.class, database)
			.compile(
				apply(
					targetAt(database, outgoing ? 1 : 0),
					and(
						eq(edgeLabel),
						incidentAt(
							var("node", HGHandle.class),
							outgoing ? 0 : 1
						)
					)
				)
			);
	}
}
