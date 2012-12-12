package info.gehrels.diplomarbeit.flockdb;

import info.gehrels.diplomarbeit.AbstractBenchmarkStep;
import info.gehrels.diplomarbeit.Measurement;
import info.gehrels.flockDBClient.FlockDB;

import static info.gehrels.diplomarbeit.Measurement.measure;

public class FlockDBBenchmarkStep extends AbstractBenchmarkStep<FlockDB> {
	public FlockDBBenchmarkStep(String algorithm, String inputPath) {
		super(algorithm, inputPath);
	}

	@Override
	protected void runImporter(final String inputPath) throws Exception {
		measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				FlockDBImporter flockDBImporter = new FlockDBImporter(inputPath);
				flockDBImporter.importNow();
				flockDBImporter.ensureImportCompleted();
			}
		});

	}

	@Override
	protected void readWholeGraph() throws Exception {
		measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				new FlockDBReadWholeGraph(FlockDBHelper.createFlockDB(), maxNodeId, true).readWholeGraph();
			}
		}, null);
	}

	@Override
	protected void calcSCC() throws Exception {
		warmUpDatabaseAndMeasure(new Measurement<FlockDB>() {
			public void execute(FlockDB flockDB) throws Exception {
				new FlockDBStronglyConnectedComponents(flockDB, maxNodeId).calculateStronglyConnectedComponents();
			}

		});
	}

	@Override
	protected void calcFoF() throws Exception {
		warmUpDatabaseAndMeasure(new Measurement<FlockDB>() {
			public void execute(FlockDB flockDB) throws Exception {
				new FlockDBFriendsOfFriends(flockDB, maxNodeId).calculateFriendsOfFriends();
			}
		});
	}

	@Override
	protected void calcCommonFriends() throws Exception {
		warmUpDatabaseAndMeasure(new Measurement<FlockDB>() {
			public void execute(FlockDB flockDB) throws Exception {
				new FlockDBCommonFriends(flockDB, maxNodeId).calculateCommonFriends();
			}
		});
	}

	@Override
	protected void calcRegularPathQueries() throws Exception {
		warmUpDatabaseAndMeasure(new Measurement<FlockDB>() {
			public void execute(FlockDB flockDB) throws Exception {
				new FlockDBRegularPathQuery(flockDB, maxNodeId).calculateRegularPaths();
			}
		});
	}

	@Override
	protected FlockDB createAndWarmUpDatabase() throws Exception {
		FlockDB flockDB = FlockDBHelper.createFlockDB();
		new FlockDBReadWholeGraph(flockDB, maxNodeId, false).readWholeGraph();
		return flockDB;
	}
}
