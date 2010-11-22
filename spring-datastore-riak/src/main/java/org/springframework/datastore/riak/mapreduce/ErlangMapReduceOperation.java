package org.springframework.datastore.riak.mapreduce;

import java.util.LinkedHashMap;
import java.util.Map;

/**
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

  public void setModule(String module) {
    moduleFunction.put("module", module);
  }

  public void setFunction(String function) {
    moduleFunction.put("function", function);
  }

  public Object getRepresentation() {
    return moduleFunction;
  }

}
