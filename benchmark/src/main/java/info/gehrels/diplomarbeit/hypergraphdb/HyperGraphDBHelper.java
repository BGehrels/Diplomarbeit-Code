package info.gehrels.diplomarbeit.hypergraphdb;

import org.hypergraphdb.HyperGraph;

import java.io.IOException;

public class HyperGraphDBHelper {
	static HyperGraph createHyperGraphDB(String dbPath) throws IOException {
		return new HyperGraph(dbPath);
	}
}
