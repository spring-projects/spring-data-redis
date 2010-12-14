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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.data.keyvalue.riak.core.RiakTemplate;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * An implementation of {@link org.springframework.core.io.UrlResource} that is backed by a
 * resource in Riak.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakResource<B, K> extends UrlResource {

  private static final Logger log = LoggerFactory.getLogger(RiakResource.class);

  private RiakTemplate riak;
  private B bucket;
  private K key;
  private String description;

  public RiakResource(RiakTemplate riak, B bucket, K key) throws MalformedURLException {
    super(riak.getDefaultUri());
    this.bucket = bucket;
    this.key = key;
  }

  public RiakTemplate getRiakTemplate() {
    return this.riak;
  }

  public B getBucket() {
    return bucket;
  }

  public K getKey() {
    return key;
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public URL getURL() throws IOException {
    try {
      return new URL(new RiakFile(riak, bucket, key).getUriAsString(true));
    } catch (URISyntaxException e) {
      log.error(e.getMessage(), e);
    }
    return null;
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public URI getURI() throws IOException {
    try {
      return new RiakFile(riak, bucket, key).toURI();
    } catch (URISyntaxException e) {
      log.error(e.getMessage(), e);
    }
    return null;
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public Resource createRelative(String relativePath) throws MalformedURLException {
    if (relativePath.startsWith("../")) {
      return new RiakResource(riak, bucket, relativePath.substring(3));
    } else if (!relativePath.startsWith("/")) {
      return new RiakResource(riak, bucket, relativePath);
    }
    return null;
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public String getFilename() {
    try {
      return new RiakFile(riak, bucket, key).getUriAsString(true);
    } catch (URISyntaxException e) {
      log.error(e.getMessage(), e);
    }
    return riak.getDefaultUri();
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public String getDescription() {
    return this.description;
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public File getFile() throws IOException {
    try {
      return new RiakFile(riak, bucket, key);
    } catch (URISyntaxException e) {
      log.error(e.getMessage(), e);
    }
    return null;
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public InputStream getInputStream() throws IOException {
    return new RiakInputStream(riak, bucket, key);
  }

}
