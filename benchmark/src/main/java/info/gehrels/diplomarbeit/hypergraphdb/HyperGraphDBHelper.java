package info.gehrels.diplomarbeit.hypergraphdb;

import org.hypergraphdb.HyperGraph;

import java.io.IOException;

public class HyperGraphDBHelper {

	static HyperGraph createHyperGraphDB(String dbPath) throws IOException {
		HyperGraph graphDb = new HyperGraph(dbPath);
		registerShutdownHook(graphDb);
		return graphDb;
	}

	private static void registerShutdownHook(final HyperGraph graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.close();
			}
		});
	}

}
