package virtuoso.jena.driver;

/**
 * mohamad@alamili.com
 */
public class TransactionFailedException extends RuntimeException {

  public TransactionFailedException(String message) {
    super(message);
  }
}
