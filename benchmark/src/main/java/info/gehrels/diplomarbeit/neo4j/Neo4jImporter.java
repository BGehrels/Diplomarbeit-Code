package info.gehrels.diplomarbeit.neo4j;

import info.gehrels.diplomarbeit.CachingImporter;
import info.gehrels.diplomarbeit.Measurement;
import info.gehrels.diplomarbeit.Node;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.LuceneBatchInserterIndexProvider;

import java.util.Map;

import static info.gehrels.diplomarbeit.Measurement.measure;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.MapUtil.genericMap;

public class Neo4jImporter extends CachingImporter<Long> {
	static final String NAME_KEY = "name";
	public static final String NODE_INDEX_NAME = "nodes";
	private final BatchInserter batchInserter;
	private final BatchInserterIndex nodeIndex;
	private final BatchInserterIndexProvider indexProvider;


	public static void main(final String... args) throws Exception {
		measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				Neo4jImporter neo4jImporter = new Neo4jImporter(args[0], args[1]);
				neo4jImporter.importNow();
				neo4jImporter.shutdown();
			}
		});
	}

	public Neo4jImporter(String sourceFile, String graphDbFolder) throws Exception {
		super(sourceFile);
		batchInserter = BatchInserters.inserter(graphDbFolder);

		indexProvider = new LuceneBatchInserterIndexProvider(batchInserter);
		nodeIndex = indexProvider.nodeIndex(NODE_INDEX_NAME, MapUtil.<String, String>genericMap("type", "exact"));
	}

	@Override
	protected void createEdgeBetweenCachedNodes(Long from, Long to, String label) throws Exception {
		batchInserter.createRelationship(from, to, withName(label),
		                                 MapUtil.<String, Object>genericMap());
	}


	@Override
	public Long createNodeForCache(Node node) {
		Map<String, Object> properties = genericMap(NAME_KEY, node.id);

		long newNode = batchInserter.createNode(properties);

		nodeIndex.add(newNode, MapUtil.<String, Object>genericMap(NAME_KEY, node.id));
		return newNode;
	}

	public void shutdown() {
		indexProvider.shutdown();
		batchInserter.shutdown();
	}
}
