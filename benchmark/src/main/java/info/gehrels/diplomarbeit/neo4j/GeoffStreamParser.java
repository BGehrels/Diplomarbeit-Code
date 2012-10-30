package info.gehrels.diplomarbeit.neo4j;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GeoffStreamParser  implements Iterable<GraphElement>{
    private static final Pattern NODE_PATTERN = Pattern.compile("\\(([^)]*)\\) \\{}");
    private static final Pattern EDGE_PATTERN = Pattern.compile("\\(([^)]*)\\)-\\[:([^]]+)\\]->\\(([^)]*)\\)");
    protected BufferedReader bufferedReader;

    public GeoffStreamParser(FileInputStream reader) {
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
                        Matcher nodeMatcher = NODE_PATTERN.matcher(line);
                        if (nodeMatcher.matches()) {
                            String id = nodeMatcher.group(1);
                            next.add(new Node(Long.valueOf(id)));
                        } else {
                            Matcher edgeMatcher = EDGE_PATTERN.matcher(line);
                            if (!edgeMatcher.matches()) {
                                System.out.println(line);
                            }
                            String id1 = edgeMatcher.group(1);
                            String label = edgeMatcher.group(2);
                            String id2 = edgeMatcher.group(3);
                            next.add(new Edge(Long.valueOf(id1), Long.valueOf(id2), label));
                        }
                    }
                }

            }


        };
    }
}
