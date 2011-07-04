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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An {@link java.io.InputStream} implementation that is backed by a resource residing in Riak.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakInputStream<B, K> extends InputStream {

  private RiakTemplate riak;
  private B bucket;
  private K key;
  private ByteArrayInputStream in;

  public RiakInputStream(RiakTemplate riak, B bucket, K key) {
    this.riak = riak;
    this.bucket = bucket;
    this.key = key;
    this.in = new ByteArrayInputStream(riak.getAsBytes(bucket, key));
  }

  @Override
  public int read(byte[] bytes) throws IOException {
    return in.read(bytes);
  }

  @Override
  public int read(byte[] bytes, int i, int i1) throws IOException {
    return in.read(bytes, i, i1);
  }

  @Override
  public long skip(long l) throws IOException {
    return in.skip(l);
  }

  @Override
  public int available() throws IOException {
    return in.available();
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public void mark(int i) {
    in.mark(i);
  }

  @Override
  public void reset() throws IOException {
    in.reset();
  }

  @Override
  public boolean markSupported() {
    return in.markSupported();
  }

  @Override
  public int read() throws IOException {
    return in.read();
  }

}
