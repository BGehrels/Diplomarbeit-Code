package info.gehrels.diplomarbeit.neo4j;

import info.gehrels.diplomarbeit.AbstractCommonFriends;
import info.gehrels.diplomarbeit.Measurement;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.diplomarbeit.neo4j.Neo4jHelper.createNeo4jDatabase;
import static info.gehrels.diplomarbeit.neo4j.Neo4jImporter.NAME_KEY;
import static info.gehrels.diplomarbeit.neo4j.Neo4jImporter.NODE_INDEX_NAME;
import static java.lang.Integer.parseInt;


public class Neo4jCommonFriends extends AbstractCommonFriends {
  public static final DynamicRelationshipType L1 = DynamicRelationshipType.withName("L1");
  private GraphDatabaseService graphDB;

  public static void main(final String[] args) throws Exception {
    measure(new Measurement<Void>() {
        @Override
        public void execute(Void database) throws Exception {
          new Neo4jCommonFriends(createNeo4jDatabase(args[0]), parseInt(args[1])).calculateCommonFriends();
        }
      });
  }

  public Neo4jCommonFriends(GraphDatabaseService neo4jDatabase, long maxNodeId) {
    super(maxNodeId);
    this.graphDB = neo4jDatabase;
  }

  @Override
  protected void calculateCommonFriends(int id1, int id2) {
    ExecutionEngine cypher = new ExecutionEngine(graphDB);
    ExecutionResult result;
    if (id1 != id2) {
      result = cypher.execute("start " +
        "n=node:" + NODE_INDEX_NAME + "(" + NAME_KEY + "={id1}), " +
        "m=node:" + NODE_INDEX_NAME + "(" + NAME_KEY + "={id2}) " +
        "match m-[:L1]->x<-[:L1]-n " +
        "return x.name as x ",
        MapUtil.<String, Object>genericMap("id1", (long) id1, "id2", (long) id2));
    } else {
      result = cypher.execute("start " +
        "m=node:" + NODE_INDEX_NAME + "(" + NAME_KEY + "={id2}) " +
        "match m-[:L1]->x " +
        "return x.name as x ",
        MapUtil.<String, Object>genericMap("id1", (long) id1, "id2", (long) id2));
    }

    for (Object x : IteratorUtil.asIterable(result.columnAs("x"))) {
      printCommonFriend((long) id1, (long) id2, (Long) x);
    }
  }

}
