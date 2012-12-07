package info.gehrels.diplomarbeit.hypergraphdb;

import com.google.common.base.Stopwatch;
import org.hypergraphdb.HyperGraph;

import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.createHyperGraphDB;

public class HyperGraphReadWholeGraph {
	private final HyperGraph graphDB;

	public HyperGraphReadWholeGraph(String dbPath) throws Exception {
		this.graphDB = createHyperGraphDB(dbPath);
		throw new UnsupportedOperationException();
	}

	public static void main(String[] args) throws Exception {
		Stopwatch stopwatch = new Stopwatch().start();
		new HyperGraphReadWholeGraph(args[0]).readWholeGraph(true);
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	public void readWholeGraph(boolean writeToStdOut) {
		throw new UnsupportedOperationException();
	}
}
