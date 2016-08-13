package virtuoso.jena.driver;

import com.hp.hpl.jena.update.UpdateException;

import java.sql.Statement;

/**
 * User: bastiaan
 * Date: 1/10/13
 * Time: 10:19
 */
public class ISQLChannel {

  public static void sendQuery(VirtGraph virtGraph, String query) {
    try {
      Statement stmt = virtGraph.createStatement();
      stmt.execute(query);
      stmt.cancel();
      stmt.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw new UpdateException("Convert results are FAILED.:", e);
    }
  }

  public static Statement executeQuery(VirtGraph virtGraph, String query) {
    try {
      Statement stmt = virtGraph.createStatement();
      return stmt;
    } catch (Exception e) {
      e.printStackTrace();
      throw new UpdateException("Convert results are FAILED.:", e);
    }
  }
}
