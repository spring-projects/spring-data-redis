/*
 * Copyright (c) 2011 by J. Brisbin <jon@jbrisbin.com>
 * Portions (c) 2011 by NPC International, Inc. or the
 * original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.keyvalue.riak.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.keyvalue.riak.core.RiakTemplate;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakClassLoader extends ClassLoader {

  protected final Log log = LogFactory.getLog(getClass());
  protected Set<String> buckets = new LinkedHashSet<String>();
  protected RiakTemplate riakTemplate;
  protected String defaultBucket = null;

  public RiakClassLoader(ClassLoader classLoader, RiakTemplate riakTemplate) {
    super(classLoader);
    init(riakTemplate);
    loadBucketsFromClassPath();
  }

  public RiakClassLoader(RiakTemplate riakTemplate) {
    init(riakTemplate);
    loadBucketsFromClassPath();
  }

  public Set<String> getBuckets() {
    return buckets;
  }

  public void setBuckets(Set<String> buckets) {
    this.buckets = buckets;
  }

  public RiakTemplate getRiakTemplate() {
    return riakTemplate;
  }

  public void setRiakTemplate(RiakTemplate riakTemplate) {
    this.riakTemplate = riakTemplate;
  }

  public String getDefaultBucket() {
    return defaultBucket;
  }

  public void setDefaultBucket(String defaultBucket) {
    this.defaultBucket = defaultBucket;
  }

  @Override
  protected Class<?> findClass(String s) throws ClassNotFoundException {
    Class<?> c;
    try {
      c = super.findClass(s);
      if (log.isDebugEnabled()) {
        log.debug(String.format("Found class '%s' locally defined.", s));
      }
    } catch (Throwable t) {
      // Class not defined in this ClassLoader yet
    }

    Set<String> buckets = new LinkedHashSet<String>(this.buckets);
    if (null != defaultBucket) {
      buckets.add(defaultBucket);
    }
    for (String bucket : buckets) {
      if (bucket.indexOf("/") < 0) {
        try {
          if (log.isDebugEnabled()) {
            log.debug(String.format("Class '%s' not locally defined, trying Riak.", s));
          }
          byte[] buff = riakTemplate.getAsBytes(URLEncoder.encode(bucket, "UTF-8"), s);
          c = defineClass(s, buff, 0, buff.length);
          if (null != c) {
            return c;
          }
        } catch (ClassFormatError ignored) {
        } catch (UnsupportedEncodingException e) {
          log.error(e.getMessage(), e);
        }
      }
    }

    // Nothing found
    throw new ClassNotFoundException("Class not found: " + s);
  }


  protected void loadBucketsFromClassPath() {
    String classPath = System.getProperty("java.class.path");
    String pathSep = System.getProperty("path.separator", ":");
    if (null != classPath) {
      String[] paths = classPath.split(pathSep);
      for (String p : paths) {
        buckets.add(p);
      }
    }
  }

  protected void init(RiakTemplate riakTemplate) {
    this.riakTemplate = riakTemplate;
    RestTemplate tmpl = this.riakTemplate.getRestTemplate();
    tmpl.getMessageConverters().add(0, new JavaSerializationMessageHandler());
    tmpl.setErrorHandler(new Ignore404sErrorHandler());
  }

  private class JavaSerializationMessageHandler implements HttpMessageConverter {

    public boolean canRead(Class clazz, MediaType mediaType) {
      return MediaType.APPLICATION_OCTET_STREAM.equals(mediaType);
    }

    public boolean canWrite(Class clazz, MediaType mediaType) {
      return null != clazz;
    }

    public List<MediaType> getSupportedMediaTypes() {
      List<MediaType> types = new ArrayList<MediaType>(1);
      types.add(MediaType.APPLICATION_OCTET_STREAM);
      return types;
    }

    public Object read(java.lang.Class clazz, HttpInputMessage inputMessage) throws
        IOException,
        HttpMessageNotReadableException {
      ObjectInputStream oin = new ObjectInputStream(inputMessage.getBody());
      try {
        Class<?> c = (Class<?>) oin.readObject();
        if (log.isDebugEnabled()) {
          log.debug("Loaded class: " + c);
        }
        return c;
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException(e.getMessage(), e);
      }
    }

    public void write(Object o, MediaType contentType, HttpOutputMessage outputMessage) throws
        IOException,
        HttpMessageNotWritableException {
      outputMessage.getHeaders().setContentType(MediaType.APPLICATION_OCTET_STREAM);
      ObjectOutputStream oout = new ObjectOutputStream(outputMessage.getBody());
      oout.writeObject(o);
      oout.flush();
    }

  }

}
