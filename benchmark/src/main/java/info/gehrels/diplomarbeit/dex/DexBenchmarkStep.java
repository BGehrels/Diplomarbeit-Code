package info.gehrels.diplomarbeit.dex;

import com.sparsity.dex.gdb.Session;
import info.gehrels.diplomarbeit.AbstractBenchmarkStep;
import info.gehrels.diplomarbeit.Measurement;
import static info.gehrels.diplomarbeit.Measurement.measure;
import static info.gehrels.diplomarbeit.dex.DEXHelper.closeDex;
import static info.gehrels.diplomarbeit.dex.DEXHelper.openDEX;


public class DexBenchmarkStep extends AbstractBenchmarkStep {
  public static final String STORAGE_FILE_NAME = "benchmark.dex";

  public DexBenchmarkStep(String algorithm, String inputPath) {
    super(algorithm, inputPath);
  }

  @Override
  protected Object createAndWarmUpDatabase() throws Exception {
    Session session = openDEX(STORAGE_FILE_NAME);
    new DEXReadWholeGraph(session, true).readWholeGraph();
    return session;
  }

  @Override
  protected void runImporter(final String inputPath) throws Exception {
    measure(new Measurement<Void>() {
        @Override
        public void execute(Void database) throws Exception {
          DEXImporter importer = new DEXImporter(inputPath, STORAGE_FILE_NAME);
          importer.importNow();
          importer.shutdown();
        }
      });
  }

  @Override
  protected void readWholeGraph() throws Exception {
    measure(new Measurement<Void>() {
        @Override
        public void execute(Void database) throws Exception {
          Session session = openDEX(STORAGE_FILE_NAME);
          new DEXReadWholeGraph(session, true).readWholeGraph();
          closeDex();
        }
      }, null);

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
