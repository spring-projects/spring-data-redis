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

import org.springframework.data.keyvalue.riak.core.BucketKeyPair;

/**
 * An implementation of {@link org.springframework.data.keyvalue.riak.mapreduce.MapReduceOperation}
 * to describe a Javascript language M/R function.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class JavascriptMapReduceOperation implements MapReduceOperation {

  protected String source;
  protected BucketKeyPair bucketKeyPair;

  public JavascriptMapReduceOperation(String source) {
    this.source = source;
  }

  public JavascriptMapReduceOperation(BucketKeyPair bucketKeyPair) {
    this.bucketKeyPair = bucketKeyPair;
  }

  public String getSource() {
    return source;
  }

  /**
   * Set the anonymous source to use for the M/R function.
   *
   * @param source
   */
  public void setSource(String source) {
    this.source = source;
  }

  public BucketKeyPair getBucketKeyPair() {
    return bucketKeyPair;
  }

  /**
   * Set the {@link org.springframework.data.keyvalue.riak.core.BucketKeyPair} to
   * point to for the Javascript to use in this M/R function.
   *
   * @param bucketKeyPair
   */
  public void setBucketKeyPair(BucketKeyPair bucketKeyPair) {
    this.bucketKeyPair = bucketKeyPair;
  }

  public Object getRepresentation() {
    return (null != bucketKeyPair ? bucketKeyPair : source);
  }

}
