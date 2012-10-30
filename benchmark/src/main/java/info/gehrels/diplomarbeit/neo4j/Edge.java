package info.gehrels.diplomarbeit.neo4j;

public class Edge implements GraphElement {
    public final long from;
    public final long to;
    public final String label;

    public Edge(long from, long to, String label) {
        this.from = from;
        this.to = to;
        this.label = label;
    }
}
