package info.gehrels.diplomarbeit.hypergraphdb;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;

import java.io.IOException;

import static org.hypergraphdb.HGQuery.hg.eq;
import static org.hypergraphdb.HGQuery.hg.var;

public class HyperGraphDBHelper {
	static HyperGraph createHyperGraphDB(String dbPath) throws IOException {
		return new HyperGraph(dbPath);
	}

	static HGQuery<HGHandle> createQueryForNodeById(HyperGraph database) {
		return hg.make(HGHandle.class, database).compile(eq(var("id")));
	}
}
