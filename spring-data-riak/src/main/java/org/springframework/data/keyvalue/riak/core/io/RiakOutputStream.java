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

package org.springframework.data.keyvalue.riak.core.io;

import org.springframework.data.keyvalue.riak.core.RiakTemplate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakOutputStream<B, K> extends ByteArrayOutputStream {

  private RiakTemplate riak;
  private B bucket;
  private K key;

  public RiakOutputStream(RiakTemplate riak, B bucket, K key) {
    this.riak = riak;
    this.bucket = bucket;
    this.key = key;
  }

  @Override
  public void flush() throws IOException {
    super.flush();
    riak.setAsBytes(bucket, key, toByteArray());
  }
}
