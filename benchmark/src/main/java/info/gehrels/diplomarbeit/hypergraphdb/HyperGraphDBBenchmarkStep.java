package info.gehrels.diplomarbeit.hypergraphdb;

import info.gehrels.diplomarbeit.AbstractBenchmarkStep;
import info.gehrels.diplomarbeit.Measurement;
import org.hypergraphdb.HyperGraph;

import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.createHyperGraphDB;

public class HyperGraphDBBenchmarkStep extends AbstractBenchmarkStep<HyperGraph> {
	private static final String HGDB_PATH = "hyperGraphDB";

	public HyperGraphDBBenchmarkStep(String algorithm, String inputPath) {
		super(algorithm, inputPath);
	}

	@Override
	protected void runImporter(final String inputPath) throws Exception {
		measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				new HyperGraphDBImporter(inputPath, HGDB_PATH).importNow();
			}
		});
	}

	@Override
	protected void readWholeGraph() throws Exception {
		measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				new HyperGraphReadWholeGraph(createHyperGraphDB(HGDB_PATH, true), true).readWholeGraph();
			}
		});
	}

	@Override
	protected void calcSCC() throws Exception {
		warmUpDatabaseAndMeasure(new Measurement<HyperGraph>() {
			public void execute(HyperGraph hyperGraph) throws Exception {
				new HyperGraphDBStronglyConnectedComponents(hyperGraph).calculateStronglyConnectedComponents();
			}
		});
	}

	@Override
	protected void calcFoF() throws Exception {
		warmUpDatabaseAndMeasure(new Measurement<HyperGraph>() {
			public void execute(HyperGraph hyperGraph) throws Exception {
				new HyperGraphDBFriendsOfFriends(hyperGraph, maxNodeId).calculateFriendsOfFriends();
			}
		});
	}

	@Override
	protected void calcCommonFriends() throws Exception {
		warmUpDatabaseAndMeasure(new Measurement<HyperGraph>() {
			public void execute(HyperGraph database) throws Exception {
				new HyperGraphCommonFriends(database, maxNodeId).calculateCommonFriends();
			}
		});
	}

	@Override
	protected void calcRegularPathQueries() throws Exception {
		warmUpDatabaseAndMeasure(new Measurement<HyperGraph>() {
			public void execute(HyperGraph database) throws Exception {
				new HyperGraphRegularPathQuery(database, maxNodeId).calculateRegularPaths();
			}
		});
	}

	protected HyperGraph createAndWarmUpDatabase() throws Exception {
		HyperGraph hyperGraph = createHyperGraphDB(HGDB_PATH, true);
		new HyperGraphReadWholeGraph(hyperGraph, false).readWholeGraph();
		return hyperGraph;
	}

}
