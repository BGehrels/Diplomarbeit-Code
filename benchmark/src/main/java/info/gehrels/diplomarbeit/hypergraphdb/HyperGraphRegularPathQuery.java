package info.gehrels.diplomarbeit.hypergraphdb;

import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.diplomarbeit.AbstractRegularPathQuery;
import info.gehrels.diplomarbeit.Measurement;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;

import java.io.IOException;

import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.getFriendNodes;
import static info.gehrels.diplomarbeit.hypergraphdb.HyperGraphDBHelper.createHyperGraphDB;
import static java.lang.Long.parseLong;
import static org.hypergraphdb.HGQuery.hg.and;
import static org.hypergraphdb.HGQuery.hg.eq;
import static org.hypergraphdb.HGQuery.hg.make;
import static org.hypergraphdb.HGQuery.hg.orderedLink;
import static org.hypergraphdb.HGQuery.hg.var;


public class HyperGraphRegularPathQuery extends AbstractRegularPathQuery {
    private final HyperGraph database;
    private HGQuery<HGHandle> aIdQuery;

    public static void main(final String[] args) throws Exception {
        Measurement.measure(new Measurement<Void>() {
            @Override
            public void execute(Void database) throws Exception {
                new HyperGraphRegularPathQuery(createHyperGraphDB(args[0], true), parseLong(args[1])).calculateRegularPaths();
            }
        });
    }

    public HyperGraphRegularPathQuery(HyperGraph database, long maxNodeId) {
        super(maxNodeId);
        this.database = database;

        this.aIdQuery = make(HGHandle.class, database).compile(eq(var("aId")));
    }

    @Override
    protected void calculateRegularPaths(int id1) throws FlockException, IOException {
        // START a=node:nodes(name={id})
        // MATCH a-[:L1]->b,
        //       b-[:L2]->c,
        //       c-[:L3]->a
        // RETURN a.name, b.name, c.name
        // ORDER BY a.name, b.name, c.name

        HGHandle a = aIdQuery.var("aId", (long) id1).findOne();
        HGSearchResult<HGHandle> cNodeCandidates = getFriendNodes(database, "L3", a, false);
        while (cNodeCandidates.hasNext()) {
            HGHandle cNodeCandidate = cNodeCandidates.next();
            HGSearchResult<HGHandle> bNodeCandidates = getFriendNodes(database, "L2", cNodeCandidate, false);

            while (bNodeCandidates.hasNext()) {
                HGHandle bNodeCandidate = bNodeCandidates.next();
                HGHandle abLink = database.findOne(and(eq("L1"), orderedLink(a, bNodeCandidate)));
                if (abLink != null) {
                    printHit((Long) database.get(a), (Long) database.get(bNodeCandidate), (Long) database.get(cNodeCandidate));
                }

            }

            bNodeCandidates.close();
        }
        
        cNodeCandidates.close();
    }

}
