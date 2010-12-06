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

import org.codehaus.groovy.runtime.GStringImpl;
import org.springframework.util.ClassUtils;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class SimpleBucketKeyResolver implements BucketKeyResolver {

  private static final boolean groovyPresent = ClassUtils.isPresent(
      "org.codehaus.groovy.runtime.GStringImpl",
      RiakTemplate.class.getClassLoader());

  protected Pattern bucketColonKey = Pattern.compile("(.+):(.+)");

  public <V> boolean canResolve(V o) {
    if (o instanceof String) {
      return true;
    } else if (o instanceof Map) {
      Map m = (Map) o;
      return (m.containsKey("bucket") && m.containsKey("key"));
    } else if (o instanceof BucketKeyPair) {
      return true;
    } else if (groovyPresent && o instanceof GStringImpl) {
      return true;
    }

    return false;
  }

  public <B, K, V> BucketKeyPair<B, K> resolve(V o) {
    BucketKeyPair<B, K> bucketKeyPair = null;

    if (o instanceof String) {
      String[] s = ((String) o).split(":");
      bucketKeyPair = new SimpleBucketKeyPair<String, String>((s.length == 1 ? null : s[0]),
          (s.length == 1 ? s[0] : s[1]));
    } else if (o instanceof Map) {
      Map m = (Map) o;
      Object bucket = m.get("bucket");
      Object key = m.get("key");
      bucketKeyPair = new SimpleBucketKeyPair<String, String>((null != bucket ? bucket
          .toString() : null),
          (null != key ? key.toString() : null));
    } else if (o instanceof BucketKeyPair) {
      bucketKeyPair = (BucketKeyPair) o;
    } else if (groovyPresent && o instanceof GStringImpl) {
      bucketKeyPair = resolve(o.toString());
    }

    return bucketKeyPair;
  }
}
