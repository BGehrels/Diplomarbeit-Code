package info.gehrels.diplomarbeit.hypergraphdb;

import com.google.common.base.Stopwatch;
import info.gehrels.diplomarbeit.AbstractReadWholeGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;

import java.util.List;

import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.createHyperGraphDB;

public class HyperGraphReadWholeGraph extends AbstractReadWholeGraph {
	private final HyperGraph graphDB;

	public HyperGraphReadWholeGraph(String dbPath, boolean writeToStdOut) throws Exception {
		super(writeToStdOut);
		this.graphDB = createHyperGraphDB(dbPath);
	}

	public static void main(String[] args) throws Exception {
		Stopwatch stopwatch = new Stopwatch().start();
		new HyperGraphReadWholeGraph(args[0], true).readWholeGraph();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	@Override
	public void readWholeGraph() {
		List<HGValueLink> all = hg.getAll(graphDB, hg.type(String.class));
		for (HGValueLink links : all) {
			write(
				graphDB.get(links.getTargetAt(0)),
				links.getValue(),
				graphDB.get(links.getTargetAt(1))
			);
		}
	}
}
