package info.gehrels.diplomarbeit.hypergraphdb;

import info.gehrels.diplomarbeit.AbstractBenchmarkStep;

public class HyperGraphDBBenchmarkStep extends AbstractBenchmarkStep {
	private static final String HGDB_PATH = "hyperGraphDB";

	public HyperGraphDBBenchmarkStep(String algorithm, String inputPath) {
		super(algorithm, inputPath);
	}

	@Override
	protected void runImporter(String inputPath) throws Exception {
		new HyperGraphDBImporter(inputPath, HGDB_PATH).importNow();
	}

	@Override
	protected void readWholeGraph() throws Exception {
		throw new UnsupportedOperationException();
		//To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	protected void calcSCC() throws Exception {
		throw new UnsupportedOperationException();
		//To change body of implemented methods use File | Settings | File Templates.
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
}
