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

package org.springframework.datastore.riak.core;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class KeyValueStoreKey {

  protected Object family;
  protected Object key;

  public KeyValueStoreKey() {
  }

  public KeyValueStoreKey(Object family, Object key) {
    this.family = family;
    this.key = key;
  }

  public Object getFamily() {
    return family;
  }

  public void setFamily(Object family) {
    this.family = family;
  }

  public Object getKey() {
    return key;
  }

  public void setKey(Object key) {
    this.key = key;
  }

  @Override
  public String toString() {
    if (null == family && null == key) {
      return super.toString();
    } else {
      return (null != family ? family.toString() : "") + ":" + (null != key ? key.toString() : "");
    }
  }
}
