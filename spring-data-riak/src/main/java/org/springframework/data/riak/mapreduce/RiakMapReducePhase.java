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

package org.springframework.data.riak.mapreduce;

/**
 * An implementation of {@link org.springframework.data.riak.mapreduce.MapReducePhase}
 * for the Riak data store.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakMapReducePhase implements MapReducePhase {

  protected Phase phase;
  protected String language;
  protected MapReduceOperation operation;
  protected boolean keepResults = false;

  public RiakMapReducePhase(String phase, String language, MapReduceOperation oper) {
    this.phase = Phase.valueOf(phase.toUpperCase());
    this.language = language;
    this.operation = oper;
  }

  public RiakMapReducePhase(Phase phase, String language, MapReduceOperation oper) {
    this.phase = phase;
    this.language = language;
    this.operation = oper;
  }

  public Phase getPhase() {
    return phase;
  }

  public String getLanguage() {
    return language;
  }

  public MapReduceOperation getOperation() {
    return this.operation;
  }

  public boolean getKeepResults() {
    return this.keepResults;
  }

  public void setKeepResults(boolean keepResults) {
    this.keepResults = keepResults;
  }

  public void setOperation(MapReduceOperation oper) {

    this.operation = oper;
  }
}
