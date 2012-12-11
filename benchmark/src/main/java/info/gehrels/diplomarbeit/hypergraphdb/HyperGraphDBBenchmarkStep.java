package info.gehrels.diplomarbeit.hypergraphdb;

import com.google.common.base.Stopwatch;
import info.gehrels.diplomarbeit.AbstractBenchmarkStep;
import info.gehrels.diplomarbeit.Measurement;
import org.hypergraphdb.HyperGraph;

import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.createHyperGraphDB;

public class HyperGraphDBBenchmarkStep extends AbstractBenchmarkStep<HyperGraph> {
	private static final String HGDB_PATH = "hyperGraphDB";

	public HyperGraphDBBenchmarkStep(String algorithm, String inputPath) {
		super(algorithm, inputPath);
	}

	@Override
	protected void runImporter(String inputPath) throws Exception {
		Stopwatch stopwatch = new Stopwatch().start();
		new HyperGraphDBImporter(inputPath, HGDB_PATH).importNow();
		stopwatch.stop();
		System.err.println(stopwatch);
	}

	@Override
	protected void readWholeGraph() throws Exception {
		Stopwatch stopwatch = new Stopwatch().start();
		new HyperGraphReadWholeGraph(createHyperGraphDB(HGDB_PATH), true).readWholeGraph();
		stopwatch.stop();
		System.err.println(stopwatch);
	}

	@Override
	protected void calcSCC() throws Exception {
		warmUpDatabaseAndMeasure(new Measurement<HyperGraph>() {
			public void execute(HyperGraph hyperGraph) throws Exception {
				new HyperGraphDBStronglyConnectedComponents(hyperGraph, maxNodeId).calculateStronglyConnectedComponents();
			}
		});
	}

	@Override
	protected void calcFoF() throws Exception {
		throw new UnsupportedOperationException();
		//To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	protected void calcCommonFriends() throws Exception {
		throw new UnsupportedOperationException();
		//To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	protected void calcRegularPathQueries() throws Exception {
		throw new UnsupportedOperationException();
		//To change body of implemented methods use File | Settings | File Templates.
	}

	protected HyperGraph createAndWarmUpDatabase() throws Exception {
		HyperGraph hyperGraph = HyperGraphDBHelper.createHyperGraphDB(HGDB_PATH);
		new HyperGraphReadWholeGraph(hyperGraph, false).readWholeGraph();
		return hyperGraph;
	}

}
