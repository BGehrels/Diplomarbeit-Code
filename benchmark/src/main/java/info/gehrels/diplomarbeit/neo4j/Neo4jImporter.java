package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.LuceneBatchInserterIndexProvider;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.MapUtil.genericMap;

public class Neo4jImporter {
    private static final String NAME_KEY = "name";
    private static final String TYPE_KEY = "type";
    static final RelationshipType TYPE = new RelationshipType() {
        @Override
        public String name() {
            return "DEFAULT";
        }
    };

    private final Map<String, Long> nodeCache = new HashMap<>(200000);
    private final BatchInserter batchInserter;
    private FileInputStream inputStream;
    private final BatchInserterIndex nodeIndex;
    private final BatchInserterIndexProvider indexProvider;


    public static void main(String... args) {

        try {
            Stopwatch stopwatch = new Stopwatch().start();
            new Neo4jImporter(args[0], args[1]).importNow().shutdown();
            stopwatch.stop();
            System.out.println(stopwatch);
        } catch (FileNotFoundException e) {
            System.out.println(e);
            System.exit(1);
        }
    }

    public Neo4jImporter(String sourceFile, String graphDbFolder) throws FileNotFoundException {
        this.inputStream = new FileInputStream(sourceFile);
        batchInserter = BatchInserters.inserter(graphDbFolder);

        indexProvider = new LuceneBatchInserterIndexProvider(batchInserter);
        nodeIndex = indexProvider.nodeIndex("nodes", MapUtil.<String, String>genericMap("type", "exact"));
    }

    public Neo4jImporter importNow() {
        for (GraphElement elem : new GeoffStreamParser(inputStream)) {
            if (elem instanceof Edge) {
                Edge edge = (Edge) elem;
                long from = nodeCache.get(edge.from);
                long to = nodeCache.get(edge.to);
                createEdge(from, to, edge.label);
            } else {
                createNode((Node) elem);
            }
        }

        return this;
    }

    private void createEdge(long from, long to, String label) {
        batchInserter.createRelationship(from, to, withName(label),
                                         MapUtil.<String, Object>genericMap("weight", label.substring(1)));
    }


    private long createNode(Node node) {
        Map<String, Object> properties = genericMap(NAME_KEY, node.name, TYPE_KEY,node.type);

        long newNode = batchInserter.createNode(properties);

        nodeIndex.add(newNode, MapUtil.<String, Object>genericMap(NAME_KEY, node.name, TYPE_KEY, node.type));
        nodeCache.put(node.id, newNode);
        return newNode;
    }

    public void shutdown() {
        indexProvider.shutdown();
        batchInserter.shutdown();
    }
}
