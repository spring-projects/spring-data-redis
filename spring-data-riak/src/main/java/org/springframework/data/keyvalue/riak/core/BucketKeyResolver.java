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
 * A generic interface to a resolver to turn a single object into a {@link
 * org.springframework.data.keyvalue.riak.core.BucketKeyPair}.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface BucketKeyResolver {

  /**
   * Can this resolver deal with the given object?
   *
   * @param o
   * @param <V>
   * @return
   */
  <V> boolean canResolve(V o);

  /**
   * Turn the given object into a BucketKeyPair.
   *
   * @param o
   * @return
   */
  <B, K, V> BucketKeyPair<B, K> resolve(V o);
}
