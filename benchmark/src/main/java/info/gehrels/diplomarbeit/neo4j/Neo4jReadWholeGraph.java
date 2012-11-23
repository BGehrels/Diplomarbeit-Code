package info.gehrels.diplomarbeit.neo4j;

import com.google.common.base.Stopwatch;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

public class Neo4jReadWholeGraph {

	private GraphDatabaseService graphDb;

	public static void main(String... args) {
		Stopwatch stopwatch = new Stopwatch().start();
		new Neo4jReadWholeGraph(args[0]).readWholeGraph();
		stopwatch.stop();
		System.out.println(stopwatch);
	}

	public Neo4jReadWholeGraph(String dbPath) {
		graphDb = Neo4jHelper.createNeo4jDatabase(dbPath);
	}

	private Neo4jReadWholeGraph readWholeGraph() {
		ExecutionEngine cypher = new ExecutionEngine(graphDb);
		ExecutionResult result = cypher.execute("start n=node(*) \n"
				+ "match n-[r]->m \n"
				+ "return n.name,type(r) as typ,m.name \n"
				+ "ORDER BY n.name,m.name,typ");

		int numberOfResults = 0;

		for (Object x : result) {
			Map<String,Long> map = (Map<String,Long>) x;
			System.out.println(map.get("n.name") + ", " + map.get("typ") + ", " + map.get("m.name"));
			numberOfResults++;
		}
		System.out.println(numberOfResults);
		return this;
	}
}
