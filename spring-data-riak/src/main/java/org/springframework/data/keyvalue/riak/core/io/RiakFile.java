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
import org.springframework.data.keyvalue.riak.DataStoreOperationException;
import org.springframework.data.keyvalue.riak.core.KeyValueStoreMetaData;
import org.springframework.data.keyvalue.riak.core.RiakTemplate;
import org.springframework.data.keyvalue.riak.core.RiakValue;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakFile<B, K> extends File {

  private static final Logger log = LoggerFactory.getLogger(RiakFile.class);

  private RiakTemplate riak;
  private B bucket;
  private K key;

  public RiakFile(RiakTemplate riak, B bucket, K key) throws URISyntaxException {
    super(riak.getDefaultUri());
    this.riak = riak;
    this.bucket = bucket;
    this.key = key;
  }

  public String getUriAsString(boolean includeKey) {
    String protocol = riak.getDefaultUri().substring(0, riak.getDefaultUri().indexOf(":"));
    String uri = String.format("%s://%s:%s%s/%s/%s",
        protocol,
        riak.getHost(),
        riak.getPort(),
        riak.getPrefix(),
        bucket,
        (includeKey ? key : ""));
    return uri;
  }

  public RiakTemplate getRiak() {
    return riak;
  }

  public void setRiak(RiakTemplate riak) {
    this.riak = riak;
  }

  public B getBucket() {
    return bucket;
  }

  public void setBucket(B bucket) {
    this.bucket = bucket;
  }

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  @Override
  public String getName() {
    return getUriAsString(true);
  }

  @Override
  public String getParent() {
    return getUriAsString(false);
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public File getParentFile() {
    try {
      return new RiakFile(riak, bucket, "");
    } catch (URISyntaxException e) {
      log.error(e.getMessage(), e);
    }
    return null;
  }

  @Override
  public String getPath() {
    return getUriAsString(true);
  }

  @Override
  public boolean isAbsolute() {
    return true;
  }

  @Override
  public String getAbsolutePath() {
    return getUriAsString(true);
  }

  @Override
  public File getAbsoluteFile() {
    return this;
  }

  @Override
  public String getCanonicalPath() throws IOException {
    return getUriAsString(true);
  }

  @Override
  public File getCanonicalFile() throws IOException {
    return this;
  }

  @SuppressWarnings({"deprecation"})
  @Override
  public URL toURL() throws MalformedURLException {
    return new URL(getUriAsString(true));
  }

  @Override
  public URI toURI() {
    try {
      return new URI(getUriAsString(true));
    } catch (URISyntaxException e) {
      log.error(e.getMessage(), e);
    }
    return null;
  }

  @Override
  public boolean canRead() {
    return true;
  }

  @Override
  public boolean canWrite() {
    return true;
  }

  @Override
  public boolean exists() {
    return riak.containsKey(bucket, key);
  }

  @Override
  public boolean isDirectory() {
    return (key != null && "".equals(key));
  }

  @Override
  public boolean isFile() {
    return (key != null && !("".equals(key)));
  }

  @Override
  public boolean isHidden() {
    return false;
  }

  @Override
  public long lastModified() {
    KeyValueStoreMetaData meta = riak.getMetaData(bucket, key);
    return (null != meta ? meta.getLastModified() : null);
  }

  @Override
  public long length() {
    return riak.getAsBytes(bucket, key).length;
  }

  @Override
  public boolean createNewFile() throws IOException {
    return true;
  }

  @Override
  public boolean delete() {
    return riak.delete(bucket, key);
  }

  @Override
  public void deleteOnExit() {
    // NO-OP
  }

  @Override
  public String[] list() {
    if (isDirectory()) {
      Map<String, Object> schema = riak.getBucketSchema(bucket, true);
      String baseUri = getUriAsString(false);
      List<String> uris = new LinkedList<String>();
      for (Object key : (List) ((Map) schema.get("props")).get("keys")) {
        uris.add(baseUri + key);
      }
      return (String[]) uris.toArray();
    }
    return new String[]{};
  }

  @Override
  public String[] list(FilenameFilter filenameFilter) {
    List<String> uris = new LinkedList<String>();
    for (String s : list()) {
      if (filenameFilter.accept(this, s)) {
        uris.add(s);
      }
    }
    return (String[]) uris.toArray();
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public File[] listFiles() {
    List<RiakFile> uris = new LinkedList<RiakFile>();
    for (String s : list()) {
      try {
        uris.add(new RiakFile(riak, bucket, s.substring(s.lastIndexOf("/"))));
      } catch (URISyntaxException e) {
        log.error(e.getMessage(), e);
      }
    }
    return (File[]) uris.toArray();
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public File[] listFiles(FilenameFilter filenameFilter) {
    List<RiakFile> uris = new LinkedList<RiakFile>();
    for (String s : list(filenameFilter)) {
      try {
        uris.add(new RiakFile(riak, bucket, s.substring(s.lastIndexOf("/"))));
      } catch (URISyntaxException e) {
        log.error(e.getMessage(), e);
      }
    }
    return (File[]) uris.toArray();
  }

  @Override
  public File[] listFiles(FileFilter fileFilter) {
    return super.listFiles(fileFilter);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public boolean mkdir() {
    return true;
  }

  @Override
  public boolean mkdirs() {
    return true;
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public boolean renameTo(File file) {
    if (file instanceof RiakFile) {
      RiakFile f = (RiakFile) file;
      RiakValue<byte[]> v = riak.getAsBytesWithMetaData(bucket, key);
      try {
        riak.setWithMetaData(f.getBucket(), f.getKey(), v.get(),
            (Map<String, String>) v.getMetaData());
      } catch (DataStoreOperationException e) {
        log.error(e.getMessage(), e);
        return false;
      }
    } else {
      throw new IllegalArgumentException("Renaming to a non-Riak file is not yet supported.");
    }
    return true;
  }

  @Override
  public boolean setLastModified(long l) {
    return false;
  }

  @Override
  public boolean setReadOnly() {
    return false;
  }

  @Override
  public boolean setWritable(boolean b, boolean b1) {
    return true;
  }

  @Override
  public boolean setWritable(boolean b) {
    return true;
  }

  @Override
  public boolean setReadable(boolean b, boolean b1) {
    return true;
  }

  @Override
  public boolean setReadable(boolean b) {
    return true;
  }

  @Override
  public boolean setExecutable(boolean b, boolean b1) {
    return false;
  }

  @Override
  public boolean setExecutable(boolean b) {
    return false;
  }

  @Override
  public boolean canExecute() {
    return false;
  }

  @Override
  public long getTotalSpace() {
    return super.getTotalSpace();
  }

  @Override
  public long getFreeSpace() {
    return super.getFreeSpace();
  }

  @Override
  public long getUsableSpace() {
    return super.getUsableSpace();
  }

  @Override
  public int compareTo(File file) {
    if (file instanceof RiakFile) {
      return 0;
    } else {
      return -1;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof RiakFile) {
      RiakFile rf = (RiakFile) o;
      return (rf.getBucket().equals(bucket) && rf.getKey().equals(key));
    }
    return false;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "@" + getUriAsString(true);
  }
}
