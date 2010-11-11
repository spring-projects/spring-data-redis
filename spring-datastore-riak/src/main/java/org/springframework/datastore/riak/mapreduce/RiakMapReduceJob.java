/*
 * Copyright (c) 2010 by J. Brisbin <jon@jbrisbin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.datastore.riak.mapreduce;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakMapReduceJob implements MapReduceJob {

  protected final Logger log = LoggerFactory.getLogger(getClass());
  protected List<?> keys = new ArrayList<Object>();
  protected List<MapReducePhase> query = new ArrayList<MapReducePhase>();
  protected Object arg;
  protected ObjectMapper mapper = new ObjectMapper();

  public MapReduceJob addInputs(List<?> keys) {
    
    return this;
  }

  public MapReduceJob addPhase(Object phase, List<?> operations) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public MapReduceJob setArg(Object arg) {
    this.arg = arg;
    return this;
  }

  public String toJson() {

    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
