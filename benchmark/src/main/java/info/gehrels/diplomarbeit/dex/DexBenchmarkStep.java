package info.gehrels.diplomarbeit.dex;

import info.gehrels.diplomarbeit.AbstractBenchmarkStep;
import info.gehrels.diplomarbeit.Measurement;
import info.gehrels.diplomarbeit.neo4j.Neo4jImporter;
import static info.gehrels.diplomarbeit.Measurement.measure;


public class DexBenchmarkStep extends AbstractBenchmarkStep {
  public DexBenchmarkStep(String algorithm, String inputPath) {
    super(algorithm, inputPath);
  }

  @Override
  protected Object createAndWarmUpDatabase() throws Exception {
    return null; //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  protected void runImporter(final String inputPath) throws Exception {
    measure(new Measurement<Void>() {
        @Override
        public void execute(Void database) throws Exception {
          DEXImporter importer = new DEXImporter(inputPath);
          importer.importNow();
          importer.shutdown();
        }
      });
  }

  @Override
  protected void readWholeGraph() throws Exception {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  protected void calcSCC() throws Exception {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  protected void calcFoF() throws Exception {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  protected void calcCommonFriends() throws Exception {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  protected void calcRegularPathQueries() throws Exception {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
