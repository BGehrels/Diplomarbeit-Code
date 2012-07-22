package info.gehrels.diplomarbeit.neo4j;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class AlibabaStreamParser implements Iterable<Edge> {
    protected BufferedReader bufferedReader;

    public AlibabaStreamParser(FileInputStream reader) {
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(reader, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Iterator<Edge> iterator() {
        return new Iterator<Edge>() {
            Edge nextEdge = null;

            @Override
            public boolean hasNext() {
                ensureNextEdgeIsFetched();
                return nextEdge != null;
            }

            @Override
            public Edge next() {
                ensureNextEdgeIsFetched();
                if (nextEdge == null) {
                    throw new NoSuchElementException();
                }

                Edge result = nextEdge;
                nextEdge = null;
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove not implemented");
            }

            private void ensureNextEdgeIsFetched() {
                if (nextEdge == null) {
                    String line;
                    try {
                        line = bufferedReader.readLine();
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }

                    if (line != null) {
                        String[] splitted = line.split(" ");
                        nextEdge = new Edge(splitted[0], splitted[1], splitted[2]);
                    }
                }

            }


        };
    }
}