package info.gehrels.diplomarbeit;

import java.io.FileInputStream;


public abstract class AbstractImporter {
  private final FileInputStream inputStream;

  public AbstractImporter(String sourceFile) throws Exception {
    this.inputStream = new FileInputStream(sourceFile);
  }

  public final void importNow() throws Exception {
    for (GraphElement elem : new GeoffStreamParser(inputStream)) {
      if (elem instanceof Edge) {
        createEdge((Edge) elem);
      } else {
        createNode((Node) elem);
      }
    }
  }

  protected abstract void createEdge(Edge edge) throws Exception;

  protected abstract void createNode(Node node);
}
