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

import org.springframework.http.MediaType;

import java.util.Date;
import java.util.Map;

/**
 * An implementation of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreMetaData}
 * for Riak.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakMetaData implements KeyValueStoreMetaData {

  private MediaType mediaType = MediaType.APPLICATION_JSON;
  private Map<String, Object> properties;

  public RiakMetaData(Map<String, Object> properties) {
    this.properties = properties;
  }

  public RiakMetaData(MediaType mediaType, Map<String, Object> properties) {
    this.mediaType = mediaType;
    this.properties = properties;
  }

  public MediaType getContentType() {
    return mediaType;
  }

  public long getLastModified() {
    return ((Date) properties.get("Last-Modified")).getTime();
  }

  public Map<String, Object> getProperties() {
    return this.properties;
  }

}
