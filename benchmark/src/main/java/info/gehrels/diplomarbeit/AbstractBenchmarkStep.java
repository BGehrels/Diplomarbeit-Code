package info.gehrels.diplomarbeit;

import com.google.common.base.Stopwatch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractBenchmarkStep<DB_TYPE> {
	public static final Pattern NUMBER_OF_NODES_PATTERN = Pattern.compile("geoff/(\\d+)_.*");
	private final String algorithm;
	private final String inputPath;
	protected long maxNodeId;

	public AbstractBenchmarkStep(String algorithm, String inputPath) {
		this.algorithm = algorithm;
		this.inputPath = inputPath;

		this.maxNodeId = getMaxNodeIfFromInputPath(inputPath);
	}

	protected static <T> void measure(Measurement<T> measurement, T hyperGraph) throws Exception {
		Stopwatch stopwatch = new Stopwatch().start();
		measurement.execute(hyperGraph);
		stopwatch.stop();
		System.err.println(stopwatch);
	}

	protected final void warmUpDatabaseAndMeasure(Measurement<DB_TYPE> measurement) throws Exception {
		DB_TYPE db = createAndWarmUpDatabase();
		measure(measurement, db);
	}

	protected abstract DB_TYPE createAndWarmUpDatabase() throws Exception;

	private long getMaxNodeIfFromInputPath(String inputPath) {
		Matcher matcher = NUMBER_OF_NODES_PATTERN.matcher(inputPath);
		matcher.find();
		return Long.parseLong(matcher.group(1)) - 1;

	}

	public final void execute() throws Exception {
		switch (algorithm) {
			case "import":
				runImporter(inputPath);
				break;
			case "readWholeGraph":
				readWholeGraph();
				break;
			case "calcSCC":
				calcSCC();
				break;
			case "calcFoF":
				calcFoF();
				break;
			case "calcCommonFriends":
				calcCommonFriends();
				break;
			case "calcRegularPathQueries":
				calcRegularPathQueries();
				break;
			default:
				throw new UnsupportedOperationException(algorithm);
		}
	}


	protected abstract void runImporter(String inputPath) throws Exception;

	protected abstract void readWholeGraph() throws Exception;

	protected abstract void calcSCC() throws Exception;

	protected abstract void calcFoF() throws Exception;

	protected abstract void calcCommonFriends() throws Exception;

	protected abstract void calcRegularPathQueries() throws Exception;
}
