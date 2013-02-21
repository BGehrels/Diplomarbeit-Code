package info.gehrels.diplomarbeit.dex;

import info.gehrels.diplomarbeit.AbstractBenchmarkStep;
import info.gehrels.diplomarbeit.Measurement;
import static info.gehrels.diplomarbeit.Measurement.measure;


public class DexBenchmarkStep extends AbstractBenchmarkStep<DexWrapper> {
  public static final String STORAGE_FILE_NAME = "benchmark.dex";

  public DexBenchmarkStep(String algorithm, String inputPath) {
    super(algorithm, inputPath);
  }

  @Override
  protected DexWrapper createAndWarmUpDatabase() throws Exception {
    DexWrapper dexWrapper = new DexWrapper(STORAGE_FILE_NAME);
    new DexReadWholeGraph(dexWrapper, true).readWholeGraph();
    return dexWrapper;
  }

  @Override
  protected void runImporter(final String inputPath) throws Exception {
    measure(new Measurement<Void>() {
        @Override
        public void execute(Void database) throws Exception {
          DexImporter importer = new DexImporter(inputPath, STORAGE_FILE_NAME);
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
          try(DexWrapper dexWrapper = new DexWrapper(STORAGE_FILE_NAME)) {
            new DexReadWholeGraph(dexWrapper, true).readWholeGraph();
          }
        }
      }, null);

  }

  @Override
  protected void calcSCC() throws Exception {
    warmUpDatabaseAndMeasure(new Measurement<DexWrapper>() {
        @Override
        public void execute(DexWrapper dexWrapper) throws Exception {
          new DexStronglyConnectedComponentsCalculator(dexWrapper).calculateStronglyConnectedComponents();
        }
      });
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
