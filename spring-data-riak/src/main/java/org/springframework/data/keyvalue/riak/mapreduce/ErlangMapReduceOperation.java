package org.springframework.data.keyvalue.riak.mapreduce;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An implementation of {@link org.springframework.data.keyvalue.riak.mapreduce.MapReduceOperation}
 * to represent an Erlang M/R function, which must be already defined inside the
 * Riak server.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class ErlangMapReduceOperation implements MapReduceOperation {

  protected String language = "erlang";
  protected Map moduleFunction = new LinkedHashMap();

  public ErlangMapReduceOperation() {
  }

  public ErlangMapReduceOperation(String module, String function) {
    setModule(module);
    setFunction(function);
  }

  /**
   * Set the Erlang module this function is defined in.
   *
   * @param module
   */
  public void setModule(String module) {
    moduleFunction.put("module", module);
  }

  /**
   * Set the name of this Erlang function.
   *
   * @param function
   */
  public void setFunction(String function) {
    moduleFunction.put("function", function);
  }

  public Object getRepresentation() {
    return moduleFunction;
  }

}
