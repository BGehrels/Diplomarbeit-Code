package info.gehrels.diplomarbeit;

import com.google.common.base.Stopwatch;

public abstract class Measurement<DB_TYPE> {
	public static <T> void measure(Measurement<T> measurement, T graphDB) throws Exception {
		Stopwatch stopwatch = new Stopwatch().start();
		measurement.execute(graphDB);
		stopwatch.stop();
		System.err.println(stopwatch);
	}

	public static void measure(Measurement<Void> measurement) throws Exception {
		measure(measurement, null);
	}

	public abstract void execute(DB_TYPE database) throws Exception;
}
