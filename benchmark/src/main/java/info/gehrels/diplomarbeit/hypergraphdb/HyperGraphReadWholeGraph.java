package info.gehrels.diplomarbeit.hypergraphdb;

import info.gehrels.diplomarbeit.AbstractReadWholeGraph;
import info.gehrels.diplomarbeit.Measurement;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;

import java.util.List;

import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.createHyperGraphDB;

public class HyperGraphReadWholeGraph extends AbstractReadWholeGraph {
	private final HyperGraph graphDB;

	public HyperGraphReadWholeGraph(HyperGraph db, boolean writeToStdOut) throws Exception {
		super(writeToStdOut);
		this.graphDB = db;
	}

	public static void main(final String[] args) throws Exception {
		measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				new HyperGraphReadWholeGraph(createHyperGraphDB(args[0]), true).readWholeGraph();
			}
		});
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
