package info.gehrels.diplomarbeit;

public interface Measurement<DB_TYPE> {
	void execute(DB_TYPE database) throws Exception;
}
