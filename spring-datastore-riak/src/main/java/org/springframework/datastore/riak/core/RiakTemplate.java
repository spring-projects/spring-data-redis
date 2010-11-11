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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.datastore.riak.convert.KeyValueStoreMetaData;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.util.Assert;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.support.RestGatewaySupport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class RiakTemplate extends RestGatewaySupport implements KeyValueStoreOperations, InitializingBean {

  protected final Logger log = LoggerFactory.getLogger(getClass());
  protected ConversionService conversionService = ConversionServiceFactory.createDefaultConversionService();
  protected String defaultUri = "http://localhost:8098/riak/{bucket}/{key}";

  public RiakTemplate() {
    setRestTemplate(new RestTemplate());

  }

  public RiakTemplate(ClientHttpRequestFactory requestFactory) {
    super(requestFactory);
  }

  public ConversionService getConversionService() {
    return conversionService;
  }

  public void setConversionService(ConversionService conversionService) {
    this.conversionService = conversionService;
  }

  public String getDefaultUri() {
    return defaultUri;
  }

  public void setDefaultUri(String defaultUri) {
    this.defaultUri = defaultUri;
  }

  public <V> KeyValueStoreOperations set(Object key, V value) {
    String[] bucketAndKey = getBucketAndKey(key);
    if (null == bucketAndKey[0]) {
      bucketAndKey[0] = value.getClass().getName();
    }
    if (null == bucketAndKey[1]) {
      // TODO: Handle auto-generation of key name
    }
    Assert.notNull(bucketAndKey[1], "Can't store an object with a NULL key.");
    RestTemplate restTemplate = getRestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(extractMediaType(value));
    HttpEntity<V> entity = new HttpEntity<V>(value, headers);
    restTemplate.put(defaultUri, entity, (Object[]) bucketAndKey);
    if (log.isDebugEnabled()) {
      log.debug(String.format("PUT object: key=%s, value=%s", key, value));
    }
    return this;
  }

  public KeyValueStoreOperations setAsBytes(Object key, byte[] value) {
    String[] bucketAndKey = getBucketAndKey(key);
    if (null == bucketAndKey[0]) {
      bucketAndKey[0] = "bytes";
    }
    if (null == bucketAndKey[1]) {
      // TODO: Handle auto-generation of key name
    }
    Assert.notNull(bucketAndKey[1], "Can't store an object with a NULL key.");
    RestTemplate restTemplate = getRestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
    HttpEntity<byte[]> entity = new HttpEntity<byte[]>(value, headers);
    restTemplate.put(defaultUri, entity, (Object[]) bucketAndKey);
    if (log.isDebugEnabled()) {
      log.debug(String.format("PUT byte[]: key=%s", key));
    }
    return this;
  }

  public <V> V get(Object key) {
    String[] bucketAndKey = getBucketAndKey(key);
    Assert.noNullElements(bucketAndKey, "Must specify a bucket and key to retrieve.");
    RestTemplate restTemplate = getRestTemplate();
    Class targetClass;
    try {
      targetClass = Class.forName(bucketAndKey[0]);
    } catch (ClassNotFoundException ignored) {
      targetClass = Map.class;
    }
    if (log.isDebugEnabled()) {
      log.debug(String.format("GET object: key=%s", key));
    }
    return (V) restTemplate.getForObject(defaultUri, targetClass, (Object[]) bucketAndKey);
  }

  public byte[] getAsBytes(Object key) {
    return getAsType(key, byte[].class);
  }

  public <T> T getAsType(Object key, Class<T> requiredType) {
    String[] bucketAndKey = getBucketAndKey(key);
    if (null == bucketAndKey[0]) {
      bucketAndKey[0] = requiredType.getName();
    }
    Assert.noNullElements(bucketAndKey, "Must specify a bucket and key to retrieve.");
    RestTemplate restTemplate = getRestTemplate();
    if (log.isDebugEnabled()) {
      log.debug(String.format("GET object: key=%s, type=%s", key, requiredType.getName()));
    }
    return (T) restTemplate.getForObject(defaultUri, requiredType, (Object[]) bucketAndKey);
  }

  public <V> V getAndSet(Object key, V value) {
    V old = (V) getAsType(key, value.getClass());
    set(key, value);
    return old;
  }

  public byte[] getAndSetBytes(Object key, byte[] value) {
    byte[] old = getAsType(key, byte[].class);
    setAsBytes(key, value);
    return old;
  }

  public <T> T getAndSetAsType(Object key, Object value, Class<T> requiredType) {
    T old = getAsType(key, requiredType);
    set(key, value);
    return old;
  }

  public List<?> getValues(List<Object> keys) {
    List<Object> results = new ArrayList<Object>();
    for (Object key : keys) {
      results.add(get(key));
    }
    return results;
  }

  public List<?> getValues(Object... keys) {
    return getValues(keys);
  }

  public <T> List<T> getValuesAsType(List<Object> keys, Class<T> requiredType) {
    List<T> results = new ArrayList<T>();
    for (Object key : keys) {
      results.add(getAsType(key, requiredType));
    }
    return results;
  }

  public <T> List<T> getValuesAsType(Class<T> requiredType, Object... keys) {
    List<Object> keyList = new ArrayList<Object>(keys.length);
    return getValuesAsType(keyList, requiredType);
  }

  public <V> KeyValueStoreOperations setIfKeyNonExistent(Object key, V value) {
    if (!containsKey(key)) {
      set(key, value);
    } else {
      if (log.isDebugEnabled()) {
        log.debug(String.format("key: %s already exists. Not adding %s", key, value));
      }
    }
    return this;
  }

  public <V> KeyValueStoreOperations setIfKeyNonExistentAsBytes(Object key, byte[] value) {
    if (!containsKey(key)) {
      setAsBytes(key, value);
    } else {
      if (log.isDebugEnabled()) {
        log.debug(String.format("key: %s already exists. Not adding %s", key, value));
      }
    }
    return this;
  }

  public KeyValueStoreOperations setMultiple(Map<Object, Object> keysAndValues) {
    for (Map.Entry<Object, Object> entry : keysAndValues.entrySet()) {
      set(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public KeyValueStoreOperations setMultipleAsBytes(Map<Object, byte[]> keysAndValues) {
    for (Map.Entry<Object, byte[]> entry : keysAndValues.entrySet()) {
      setAsBytes(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public KeyValueStoreOperations setMultipleIfKeysNonExistent(Map<Object, Object> keysAndValues) {
    for (Map.Entry<Object, Object> entry : keysAndValues.entrySet()) {
      setIfKeyNonExistent(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public KeyValueStoreOperations setMultipleAsBytesIfKeysNonExistent(Map<Object, byte[]> keysAndValues) {
    for (Map.Entry<Object, byte[]> entry : keysAndValues.entrySet()) {
      setIfKeyNonExistentAsBytes(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public boolean containsKey(Object key) {
    String[] bucketAndKey = getBucketAndKey(key);
    Assert.noNullElements(bucketAndKey, "Must specify a bucket and key to check for.");
    RestTemplate restTemplate = getRestTemplate();
    HttpHeaders headers = null;
    try {
      headers = restTemplate.headForHeaders(defaultUri, (Object[]) bucketAndKey);
    } catch (ResourceAccessException e) {
    }
    return (null != headers);
  }

  public boolean deleteKeys(Object... keys) {
    boolean deleted = false;
    for (Object key : keys) {
      String[] bucketAndKey = getBucketAndKey(key);
      Assert.noNullElements(bucketAndKey, "Must specify a bucket and key to delete.");
      RestTemplate restTemplate = getRestTemplate();
      restTemplate.delete(defaultUri, (Object[]) bucketAndKey);
      deleted = (!deleted && containsKey(key) ? false : true);
    }
    return deleted;
  }

  public void afterPropertiesSet() throws Exception {
    Assert.notNull(conversionService, "Must specify a valid ConversionService.");
  }

  protected String[] getBucketAndKey(Object obj) {
    Object bucket = null;
    Object key = null;
    if (obj instanceof Map) {
      Map m = (Map) obj;
      bucket = m.get("bucket");
      key = m.get("key");
    } else {
      String s = obj.toString();
      if (s.contains("@")) {
        // This is likely the result of Object.toString()
        // which returns com.mypackage.MyObject@memaddr
        // Convert it using the conversion service if that's the case
        s = conversionService.convert(obj, String.class);
      }
      if (s.contains(":")) {
        String[] a = s.split(":");
        bucket = a[0];
        key = a[1];
      } else {
        bucket = null;
        key = s;
      }
    }
    return new String[]{(null != bucket ? bucket.toString() : null), (null != key ? key.toString() : null)};
  }

  protected MediaType extractMediaType(Object value) {
    MediaType mediaType = (value instanceof byte[] ? MediaType.APPLICATION_OCTET_STREAM : MediaType.APPLICATION_JSON);
    if (value.getClass().getAnnotations().length > 0) {
      KeyValueStoreMetaData meta = value.getClass().getAnnotation(KeyValueStoreMetaData.class);
      if (null != meta) {
        mediaType = MediaType.parseMediaType(meta.mediaType());
      }
    }
    return mediaType;
  }

}
