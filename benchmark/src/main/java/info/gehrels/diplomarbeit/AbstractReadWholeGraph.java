package info.gehrels.diplomarbeit;

public abstract class AbstractReadWholeGraph {
	protected final boolean writeToStdOut;

	public AbstractReadWholeGraph(boolean writeToStdOut) {
		this.writeToStdOut = writeToStdOut;
	}

	public abstract void readWholeGraph() throws Exception;

	protected final void write(Object startNodeName, Object type, Object endNodeName) {
		if (writeToStdOut) {
			System.out.println(startNodeName + ", " + type + ", " + endNodeName);
		}
	}
}
