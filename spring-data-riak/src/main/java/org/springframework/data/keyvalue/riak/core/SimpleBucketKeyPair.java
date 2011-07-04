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

package org.springframework.data.keyvalue.riak.core;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class SimpleBucketKeyPair<B, K> implements BucketKeyPair, Comparable {

  private Object bucket;
  private Object key;

  public SimpleBucketKeyPair(B bucket, K key) {
    this.bucket = bucket;
    this.key = key;
  }

  public B getBucket() {
    return (B) bucket;
  }

  public K getKey() {
    return (K) key;
  }

  public int compareTo(Object o) {
    if (o instanceof SimpleBucketKeyPair) {
      SimpleBucketKeyPair pair = (SimpleBucketKeyPair) o;
      if (pair.getBucket().equals(bucket) && pair.getKey().equals(key)) {
        return 0;
      }
    }
    return -1;
  }

  @Override
  public String toString() {
    return String.format("{bucket=%s, key=%s}", bucket, key);
  }
}
