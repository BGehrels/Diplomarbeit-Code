package info.gehrels.diplomarbeit.hypergraphdb;

import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.diplomarbeit.AbstractRegularPathQuery;
import info.gehrels.diplomarbeit.Measurement;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;

import java.io.IOException;

import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.createHyperGraphDB;
import static java.lang.Long.parseLong;

public class HyperGraphRegularPathQuery extends AbstractRegularPathQuery {
	private final HyperGraph database;

	public static void main(final String[] args) throws Exception {
		Measurement.measure(new Measurement<Void>() {
			@Override
			public void execute(Void database) throws Exception {
				new HyperGraphRegularPathQuery(createHyperGraphDB(args[0]), parseLong(args[1])).calculateRegularPaths();
			}
		});
	}

	public HyperGraphRegularPathQuery(HyperGraph database, long maxNodeId) {
		super(maxNodeId);
		this.database = database;
	}

	// TODO: Impliziert hg.eq("L3") hg.type(String.class)?
	// TODO: Kantenlabels indizieren?
	@Override
	protected void calculateRegularPaths(int id1) throws FlockException, IOException {
		// START a=node:nodes(name={id})
		// MATCH a-[:L1]->b,
		//       b-[:L2]->c,
		//       c-[:L3]->a
		// RETURN a.name, b.name, c.name
		// ORDER BY a.name, b.name, c.name

		HGHandle a = database.findOne(hg.eq((long) id1));
		HGSearchResult<HGHandle> caLinks = database.find(hg.and(hg.eq("L3"), hg.incidentAt(a, 1)));
		while (caLinks.hasNext()) {
			HGHandle c = ((HGValueLink) database.get(caLinks.next())).getTargetAt(0);
			HGSearchResult<HGHandle> bcLinks = database.find(hg.and(hg.eq("L2"), hg.incidentAt(c, 1)));

			while (bcLinks.hasNext()) {
				HGHandle b = ((HGValueLink) database.get(bcLinks.next())).getTargetAt(0);
				HGHandle abLink = database.findOne(hg.and(hg.eq("L1"), hg.incidentAt(a, 0), hg.incidentAt(b, 1)));
				if (abLink != null) {
					printHit((Long) database.get(a), (Long) database.get(b), (Long)database.get(c));
				}

			}

			bcLinks.close();
		}

		caLinks.close();
	}
}
