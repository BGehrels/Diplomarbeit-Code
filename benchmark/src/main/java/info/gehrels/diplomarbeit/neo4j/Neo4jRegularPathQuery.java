package info.gehrels.diplomarbeit.neo4j;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;

import java.util.Map;

import static java.lang.Integer.parseInt;

public class Neo4jRegularPathQuery extends AbstractRegularPathQuery<GraphDatabaseService> {

	public Neo4jRegularPathQuery(String dbPath, int maxNodeId) {
		super(maxNodeId, Neo4jHelper.createNeo4jDatabase(dbPath));
	}

	public static void main(String[] args) throws Exception {
		new Neo4jRegularPathQuery(args[0], parseInt(args[1])).calculateRegularPaths();
	}

	@Override
	protected void calculateRegularPaths(int id1) {
		ExecutionEngine cypher = new ExecutionEngine(graphDB);
		ExecutionResult result = cypher.execute("START a=node:nodes(name={id})\n"
		                                        + "MATCH a-[:L1]->b,\n"
		                                        + "      b-[:L2]->c,\n"
		                                        + "      c-[:L3]->a\n"
		                                        + "RETURN a.name, b.name, c.name\n"
		                                        + "ORDER BY a.name, b.name, c.name",
		                                        MapUtil.<String, Object>genericMap("id", Integer.toString(id1)));


		for (Object x : result) {
			Map<String,Long> map = (Map<String,Long>) x;
			System.out.println(map.get("a.name") + ", " + map.get("b.name") + ", " + map.get("c.name"));
		}
	}
}
