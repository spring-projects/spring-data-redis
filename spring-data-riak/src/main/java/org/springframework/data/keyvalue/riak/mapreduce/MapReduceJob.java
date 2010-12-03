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

import java.util.List;
import java.util.concurrent.Callable;

/**
 * A generic interface to representing a Map/Reduce job to a data store that
 * supports that operation.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface MapReduceJob<T> extends Callable {

  /**
   * Get the list of inputs for this job.
   *
   * @return
   */
  <V> List<V> getInputs();

  /**
   * Set the list of inputs for this job.
   *
   * @param keys
   * @param <V>
   * @return
   */
  <V> MapReduceJob addInputs(List<V> keys);

  /**
   * Add a phase to this operation.
   *
   * @param phase
   * @return
   */
  MapReduceJob addPhase(MapReducePhase phase);

  /**
   * Set the static argument for this job.
   *
   * @param arg
   */
  void setArg(T arg);

  /**
   * Get the static argument for this job.
   *
   * @param <T>
   * @return
   */
  <T> T getArg();

  /**
   * Convert this job into the appropriate JSON to send to the server.
   *
   * @return
   */
  String toJson();
}
