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

package org.springframework.data.keyvalue.riak.mapreduce;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Generic interface to Map/Reduce in data stores that support it.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface MapReduceOperations {

  /**
   * Execute a {@link org.springframework.data.keyvalue.riak.mapreduce.MapReduceJob}
   * synchronously.
   *
   * @param job
   * @return
   */
  Object execute(MapReduceJob job);

  /**
   * Execute a MapReduceJob synchronously, converting the result into the given
   * type.
   *
   * @param job
   * @param targetType
   * @return The converted value.
   */
  <T> T execute(MapReduceJob job, Class<T> targetType);

  /**
   * Submit the job to run asynchronously.
   *
   * @param job
   * @return The Future representing the submitted job.
   */
  <T> Future<List<T>> submit(MapReduceJob job);

}
