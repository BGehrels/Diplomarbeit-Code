package info.gehrels.diplomarbeit.neo4j;

import info.gehrels.diplomarbeit.AbstractRegularPathQuery;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;

import java.util.Map;

import static java.lang.Integer.parseInt;

public class Neo4jRegularPathQuery extends AbstractRegularPathQuery {

	private final GraphDatabaseService graphDB;

	public Neo4jRegularPathQuery(GraphDatabaseService neo4jDatabase, long maxNodeId) {
		super(maxNodeId);
		graphDB = neo4jDatabase;
	}

	public static void main(String[] args) throws Exception {
		new Neo4jRegularPathQuery(Neo4jHelper.createNeo4jDatabase(args[0]), parseInt(args[1])).calculateRegularPaths();
	}

	@Override
	protected void calculateRegularPaths(int id1) {
		ExecutionEngine cypher = new ExecutionEngine(graphDB);
		ExecutionResult result = cypher.execute("START a=node:nodes(name={id})\n"
		                                        + "MATCH a-[:L1]->b,\n"
		                                        + "      b-[:L2]->c,\n"
		                                        + "      c-[:L3]->a\n"
		                                        + "RETURN a.name, b.name, c.name",
		                                        MapUtil.<String, Object>genericMap("id", Integer.toString(id1)));


		for (Object x : result) {
			Map<String,Long> map = (Map<String,Long>) x;
			printHit(map.get("a.name"), map.get("b.name"), map.get("c.name"));
		}
	}

}
