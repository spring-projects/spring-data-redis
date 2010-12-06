/*
 * Copyright (c) 2010 by J. Brisbin <jon@jbrisbin.com>
 *     Portions (c) 2010 by NPC International, Inc. or the
 *     original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
