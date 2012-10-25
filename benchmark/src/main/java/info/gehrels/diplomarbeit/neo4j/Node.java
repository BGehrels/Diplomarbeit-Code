package info.gehrels.diplomarbeit.neo4j;

public class Node implements GraphElement {
    public final String id;
    public final String name;
    public final String type;

    public Node(String id) {
        this.id = id;
        this.name = null;
        this.type = null;
    }

    public Node(String id, String name, String type) {
        //To change body of created methods use File | Settings | File Templates.
        this.id = id;
        this.name = name;
        this.type = type;
    }
}
