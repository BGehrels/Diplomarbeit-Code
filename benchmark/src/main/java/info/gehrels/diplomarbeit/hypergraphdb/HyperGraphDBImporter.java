package info.gehrels.diplomarbeit.hypergraphdb;

import com.google.common.base.Stopwatch;
import info.gehrels.diplomarbeit.CachingImporter;
import info.gehrels.diplomarbeit.Node;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;

import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.createHyperGraphDB;

public class HyperGraphDBImporter extends CachingImporter<HGHandle> {
	private final HyperGraph hyperGraph;

	public static void main(String[] args) throws Exception {
		Stopwatch stopwatch = new Stopwatch().start();
		new HyperGraphDBImporter(args[0], args[1]).importNow();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	public HyperGraphDBImporter(String sourceFile, String dbPath) throws Exception {
		super(sourceFile);
		hyperGraph = createHyperGraphDB(dbPath);
	}

	@Override
	protected void createEdgeBetweenCachedNodes(HGHandle from, HGHandle to, String label) throws Exception {
		hyperGraph.add(new HGValueLink(label, from, to));
	}

	@Override
	protected HGHandle createNodeForCache(Node node) {
		return hyperGraph.add(node.id);
	}
}