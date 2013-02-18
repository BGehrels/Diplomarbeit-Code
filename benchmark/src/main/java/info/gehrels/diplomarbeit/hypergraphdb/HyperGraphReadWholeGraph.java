package info.gehrels.diplomarbeit.hypergraphdb;

import info.gehrels.diplomarbeit.AbstractReadWholeGraph;
import info.gehrels.diplomarbeit.Measurement;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;

import java.util.List;

import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.createHyperGraphDB;
import static org.hypergraphdb.HGQuery.hg.getAll;
import static org.hypergraphdb.HGQuery.hg.type;

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
				new HyperGraphReadWholeGraph(createHyperGraphDB(args[0], true), true).readWholeGraph();
			}
		});
	}

	@Override
	public void readWholeGraph() {
		List<HGValueLink> all = getAll(graphDB, type(String.class));
		for (HGValueLink link : all) {
			write(
				graphDB.get(link.getTargetAt(0)),
				link.getValue(),
				graphDB.get(link.getTargetAt(1))
			);
		}
	}
}
