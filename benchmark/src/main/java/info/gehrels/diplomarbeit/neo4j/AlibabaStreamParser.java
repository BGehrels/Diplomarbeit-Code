package info.gehrels.diplomarbeit.neo4j;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class AlibabaStreamParser implements Iterable<GraphElement> {
    private Set<Long> nodesAlreadyParsed = new HashSet<>();
    protected BufferedReader bufferedReader;

    public AlibabaStreamParser(FileInputStream reader) {
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(reader, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Iterator<GraphElement> iterator() {
        return new QueueBasedLazyIterator<GraphElement>() {
            @Override
            protected void ensureNextElementIsFetched() {
                if (next.isEmpty()) {
                    String line;
                    try {
                        line = bufferedReader.readLine();
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }

                    if (line != null && !line.isEmpty()) {
                        String[] splitted = line.split(" ");
                        addToNextQueue(Long.valueOf(splitted[0]));
                        addToNextQueue(Long.valueOf(splitted[1]));
                        next.add(new Edge(Long.valueOf(splitted[0]), Long.valueOf(splitted[1]), splitted[2]));
                    }
                }

            }

            private void addToNextQueue(long nodeId) {
                if (!nodesAlreadyParsed.contains(nodeId)) {
                    next.add(new Node(nodeId));
                    nodesAlreadyParsed.add(nodeId);
                }
            }


        };
    }
}