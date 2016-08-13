package virtuoso.jena.driver;

import java.sql.SQLException;

public class Virtuoso {

  private String address, username, password;
  private VirtGraph virtGraph;

  public Virtuoso(String address, String username, String password) {
    this.address = address;
    this.username = username;
    this.password = password;
  }

  // Only one connection is allowed
  public VirtGraph getVirtGraph() {
    try {
      if (virtGraph == null || virtGraph.getConnection().isClosed()) {
        virtGraph = new VirtGraph(address, username, password);

        //This is a very important setting. It makes sure that for some queries, we query all graphs
        virtGraph.setReadFromAllGraphs(true);
      }
    } catch (SQLException e) {
      virtGraph = null;
      throw new RuntimeException(e.getMessage());
    }

    return virtGraph;
  }

  public void close() {
    if(virtGraph != null) {
      virtGraph.close();
    }
  }
}