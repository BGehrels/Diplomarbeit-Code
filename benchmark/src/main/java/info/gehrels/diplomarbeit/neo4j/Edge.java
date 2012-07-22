package info.gehrels.diplomarbeit.neo4j;

public class Edge {
    public final String from;
    public final String to;
    public final String label;

    public Edge(String from, String to, String label) {
        this.from = from;
        this.to = to;
        this.label = label;
    }
}
