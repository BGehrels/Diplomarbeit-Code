package info.gehrels.diplomarbeit.dex;

import com.sparsity.dex.gdb.Database;
import com.sparsity.dex.gdb.Dex;
import com.sparsity.dex.gdb.DexConfig;
import com.sparsity.dex.gdb.Session;
import java.io.FileNotFoundException;


public class DEXHelper {
  private static Dex dex;
  private static Database database;
  private static Session session;

  static Session openDEX(String storageFileName) throws FileNotFoundException {
    DexConfig dexConfig = new DexConfig();
    dex = new Dex(dexConfig);
    database = dex.open(storageFileName, true);
    session = database.newSession();
    return session;
  }

  static void closeDex() {
    session.close();
    database.close();
    dex.close();
  }
}
