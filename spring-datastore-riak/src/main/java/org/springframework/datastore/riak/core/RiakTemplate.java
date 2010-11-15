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

import org.codehaus.groovy.runtime.GStringImpl;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ser.CustomSerializerFactory;
import org.codehaus.jackson.map.ser.ToStringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.datastore.riak.DataStoreOperationException;
import org.springframework.datastore.riak.convert.KeyValueStoreMetaData;
import org.springframework.datastore.riak.mapreduce.MapReduceJob;
import org.springframework.datastore.riak.mapreduce.MapReduceOperations;
import org.springframework.datastore.riak.mapreduce.RiakMapReduceJob;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJacksonHttpMessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.support.RestGatewaySupport;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class RiakTemplate extends RestGatewaySupport implements KeyValueStoreOperations, MapReduceOperations, InitializingBean {

  private static final boolean groovyPresent = ClassUtils.isPresent("org.codehaus.groovy.runtime.GStringImpl",
      RiakTemplate.class.getClassLoader());
  protected final Logger log = LoggerFactory.getLogger(getClass());
  protected ConversionService conversionService = ConversionServiceFactory.createDefaultConversionService();
  protected ConcurrentSkipListMap<Object, Object> cache = new ConcurrentSkipListMap<Object, Object>();
  protected ObjectMapper mapper = new ObjectMapper();
  protected ExecutorService queue = Executors.newCachedThreadPool();

  protected String defaultUri = "http://localhost:8098/riak/{bucket}/{key}";
  protected String mapReduceUri = "http://localhost:8098/mapred";
  protected List<BucketKeyResolver> bucketKeyResolvers;

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

  public String getMapReduceUri() {
    return mapReduceUri;
  }

  public void setMapReduceUri(String mapReduceUri) {
    this.mapReduceUri = mapReduceUri;
  }

  public List<BucketKeyResolver> getBucketKeyResolvers() {
    return bucketKeyResolvers;
  }

  public void setBucketKeyResolvers(List<BucketKeyResolver> bucketKeyResolvers) {
    this.bucketKeyResolvers = bucketKeyResolvers;
  }

  public <K, V> KeyValueStoreOperations set(K key, V value) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, value);
    RestTemplate restTemplate = getRestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(extractMediaType(value));
    HttpEntity<V> entity = new HttpEntity<V>(value, headers);
    restTemplate.put(defaultUri, entity, bucketKeyPair.getBucket(), bucketKeyPair.getKey());
    if (log.isDebugEnabled()) {
      log.debug(String.format("PUT object: bucket=%s, key=%s, value=%s",
          bucketKeyPair.getBucket(),
          bucketKeyPair.getKey(),
          value));
    }
    return this;
  }

  public <K> KeyValueStoreOperations setAsBytes(K key, byte[] value) {
    Assert.notNull(key, "Can't store an object with a NULL key.");
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, value);
    String bucketName = (null != bucketKeyPair.getBucket() ? bucketKeyPair.getBucket().toString() : "bytes");
    RestTemplate restTemplate = getRestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
    HttpEntity<byte[]> entity = new HttpEntity<byte[]>(value, headers);
    restTemplate.put(defaultUri, entity, bucketName, bucketKeyPair.getKey());
    if (log.isDebugEnabled()) {
      log.debug(String.format("PUT byte[]: bucket=%s, key=%s", bucketKeyPair.getBucket(), bucketKeyPair.getKey()));
    }
    return this;
  }

  public <K, V> V get(K key) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    RestTemplate restTemplate = getRestTemplate();
    Class targetClass;
    try {
      targetClass = Class.forName(bucketKeyPair.getBucket().toString());
    } catch (Throwable ignored) {
      targetClass = Map.class;
    }
    String bucketName = (null != bucketKeyPair.getBucket() ? bucketKeyPair.getBucket()
        .toString() : targetClass.getName());
    if (log.isDebugEnabled()) {
      log.debug(String.format("GET object: bucket=%s, key=%s", bucketName, bucketKeyPair.getKey()));
    }
    try {
      return (V) restTemplate.getForObject(defaultUri, targetClass, bucketName, bucketKeyPair.getKey());
    } catch (HttpClientErrorException e) {
      if (e.getStatusCode() != HttpStatus.NOT_FOUND) {
        throw new DataAccessResourceFailureException(e.getMessage(), e);
      }
      return null;
    }
  }

  public <K> byte[] getAsBytes(K key) {
    return getAsType(key, byte[].class);
  }

  public <K, T> T getAsType(K key, Class<T> requiredType) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, requiredType);
    String bucketName = (null != bucketKeyPair.getBucket() ? bucketKeyPair.getBucket()
        .toString() : requiredType.getName());
    RestTemplate restTemplate = getRestTemplate();
    if (log.isDebugEnabled()) {
      log.debug(String.format("GET object: bucket=%s, key=%s, type=%s",
          bucketName,
          bucketKeyPair.getKey(),
          requiredType.getName()));
    }
    try {
      return (T) restTemplate.getForObject(defaultUri, requiredType, bucketName, bucketKeyPair.getKey());
    } catch (HttpClientErrorException e) {
      if (e.getStatusCode() != HttpStatus.NOT_FOUND) {
        throw new DataAccessResourceFailureException(e.getMessage(), e);
      }
      return null;
    }
  }

  public <K, V> V getAndSet(K key, V value) {
    V old = (V) getAsType(key, value.getClass());
    set(key, value);
    return old;
  }

  public <K> byte[] getAndSetAsBytes(K key, byte[] value) {
    byte[] old = getAsType(key, byte[].class);
    setAsBytes(key, value);
    return old;
  }

  public <K, V, T> T getAndSetAsType(K key, V value, Class<T> requiredType) {
    T old = getAsType(key, requiredType);
    set(key, value);
    return old;
  }

  public <K, V> List<V> getValues(List<K> keys) {
    List<V> results = new ArrayList<V>();
    for (K key : keys) {
      BucketKeyPair bkp = resolveBucketKeyPair(key, null);
      results.add((V) get(bkp));
    }
    return results;
  }

  public <K, V> List<V> getValues(K... keys) {
    return getValues(keys);
  }

  public <K, T> List<T> getValuesAsType(List<K> keys, Class<T> requiredType) {
    List<T> results = new ArrayList<T>();
    for (K key : keys) {
      BucketKeyPair bkp = resolveBucketKeyPair(key, null);
      results.add(getAsType(bkp, requiredType));
    }
    return results;
  }

  public <T, K> List<T> getValuesAsType(Class<T> requiredType, K... keys) {
    List<K> keyList = new ArrayList<K>(keys.length);
    return getValuesAsType(keyList, requiredType);
  }

  public <K, V> KeyValueStoreOperations setIfKeyNonExistent(K key, V value) {
    if (!containsKey(key)) {
      set(key, value);
    } else {
      if (log.isDebugEnabled()) {
        log.debug(String.format("key: %s already exists. Not adding %s", key, value));
      }
    }
    return this;
  }

  public <K> KeyValueStoreOperations setIfKeyNonExistentAsBytes(K key, byte[] value) {
    if (!containsKey(key)) {
      setAsBytes(key, value);
    } else {
      if (log.isDebugEnabled()) {
        log.debug(String.format("key: %s already exists. Not adding %s", key, value));
      }
    }
    return this;
  }

  public <K, V> KeyValueStoreOperations setMultiple(Map<K, V> keysAndValues) {
    for (Map.Entry<K, V> entry : keysAndValues.entrySet()) {
      set(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public <K> KeyValueStoreOperations setMultipleAsBytes(Map<K, byte[]> keysAndValues) {
    for (Map.Entry<K, byte[]> entry : keysAndValues.entrySet()) {
      setAsBytes(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public <K, V> KeyValueStoreOperations setMultipleIfKeysNonExistent(Map<K, V> keysAndValues) {
    for (Map.Entry<K, V> entry : keysAndValues.entrySet()) {
      setIfKeyNonExistent(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public <K> KeyValueStoreOperations setMultipleAsBytesIfKeysNonExistent(Map<K, byte[]> keysAndValues) {
    for (Map.Entry<K, byte[]> entry : keysAndValues.entrySet()) {
      setIfKeyNonExistentAsBytes(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public <K> boolean containsKey(K key) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    RestTemplate restTemplate = getRestTemplate();
    HttpHeaders headers = null;
    try {
      headers = restTemplate.headForHeaders(defaultUri, bucketKeyPair.getBucket(), bucketKeyPair.getKey());
    } catch (ResourceAccessException e) {
    }
    return (null != headers);
  }

  public <K> boolean deleteKeys(K... keys) {
    boolean stillExists = false;
    RestTemplate restTemplate = getRestTemplate();
    for (K key : keys) {
      BucketKeyPair bkp = resolveBucketKeyPair(key, null);
      try {
        restTemplate.delete(defaultUri, bkp.getBucket(), bkp.getKey());
      } catch (HttpClientErrorException e) {
        if (e.getStatusCode() != HttpStatus.NOT_FOUND) {
          throw new DataAccessResourceFailureException(e.getMessage(), e);
        }
      }
      if (!stillExists) {
        stillExists = containsKey(key);
      }
    }
    return !stillExists;
  }

  /*----------------- Map/Reduce Operations -----------------*/

  public RiakMapReduceJob createMapReduceJob() {
    return new RiakMapReduceJob(this);
  }

  public Object execute(MapReduceJob job) {
    return execute(job, List.class);
  }

  public <T> T execute(MapReduceJob job, Class<T> targetType) {
    RestTemplate restTemplate = getRestTemplate();
    ResponseEntity<T> resp = restTemplate.postForEntity(mapReduceUri, job.toJson(), targetType);
    if (resp.hasBody()) {
      return resp.getBody();
    }
    return null;
  }

  public <T> Future<List<T>> submit(MapReduceJob job) {
    return queue.submit(job);
  }

  public void afterPropertiesSet() throws Exception {
    Assert.notNull(conversionService, "Must specify a valid ConversionService.");
    if (null == bucketKeyResolvers) {
      bucketKeyResolvers = new ArrayList<BucketKeyResolver>();
      bucketKeyResolvers.add(new SimpleBucketKeyResolver());
    }

    if (groovyPresent) {
      // Native conversion for Groovy GString objects
      List<HttpMessageConverter<?>> converters = getRestTemplate().getMessageConverters();
      for (HttpMessageConverter converter : converters) {
        if (converter instanceof MappingJacksonHttpMessageConverter) {
          ObjectMapper mapper = new ObjectMapper();
          CustomSerializerFactory fac = new CustomSerializerFactory();
          fac.addSpecificMapping(GStringImpl.class, ToStringSerializer.instance);
          mapper.setSerializerFactory(fac);
          ((MappingJacksonHttpMessageConverter) converter).setObjectMapper(mapper);
        }
      }
    }
  }

  protected BucketKeyPair resolveBucketKeyPair(Object key, Object val) {
    BucketKeyResolver resolver = null;
    for (BucketKeyResolver r : bucketKeyResolvers) {
      if (r.canResolve(key)) {
        resolver = r;
        break;
      }
    }
    BucketKeyPair bucketKeyPair;
    if (null != resolver) {
      bucketKeyPair = resolver.resolve(key);
      if (null != val) {
        Annotation meta = (val instanceof Class ? (Class) val : val.getClass()).getAnnotation(KeyValueStoreMetaData.class);
        if (null != meta) {
          String bucket = ((KeyValueStoreMetaData) meta).bucket();
          if (null != bucket) {
            return new SimpleBucketKeyPair<String, Object>(bucket, bucketKeyPair.getKey());
          }
        }
      }
      return bucketKeyPair;
    }
    throw new DataStoreOperationException(String.format("No resolvers available to resolve bucket/key pair from %s",
        key));
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
