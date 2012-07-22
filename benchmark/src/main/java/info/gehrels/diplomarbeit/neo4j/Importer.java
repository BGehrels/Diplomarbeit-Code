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

import static org.neo4j.helpers.collection.MapUtil.genericMap;

public class Importer {
    private static final String NAME = "name";
    private static final RelationshipType TYPE = new RelationshipType() {
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
            new Importer(args[0], args[1]).importNow().shutdown();
            stopwatch.stop();
            System.out.println(stopwatch);
        } catch (FileNotFoundException e) {
            System.out.println(e);
            System.exit(1);
        }
    }

    public Importer(String sourceFile, String graphDbFolder) throws FileNotFoundException {
        this.inputStream = new FileInputStream(sourceFile);
        batchInserter = BatchInserters.inserter(graphDbFolder);

        indexProvider = new LuceneBatchInserterIndexProvider(batchInserter);
        nodeIndex = indexProvider.nodeIndex("nodes", MapUtil.<String, String>genericMap("type", "exact"));
    }

    public Importer importNow() {
        for (Edge edge : new AlibabaStreamParser(inputStream)) {
            long from = createNode(edge.from);
            long to = createNode(edge.to);
            createEdge(from, to, edge.label);
        }

        return this;
    }

    private void createEdge(long from, long to, String label) {
        batchInserter.createRelationship(from, to, TYPE, MapUtil.<String, Object>genericMap("label", label));
    }


    private long createNode(String nodeName) {
        Long cachedNode = nodeCache.get(nodeName);
        if (cachedNode != null) {
            return cachedNode;
        }

        Map<String, Object> properties = genericMap(NAME, nodeName);

        long newNode = batchInserter.createNode(properties);

        nodeIndex.add(newNode, MapUtil.<String, Object>genericMap("name", nodeName));
        nodeCache.put(nodeName, newNode);
        return newNode;
    }

    public void shutdown() {
        indexProvider.shutdown();
        batchInserter.shutdown();
    }
}
