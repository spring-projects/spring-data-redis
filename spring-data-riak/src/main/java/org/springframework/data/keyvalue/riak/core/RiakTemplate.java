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
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ser.CustomSerializerFactory;
import org.codehaus.jackson.map.ser.ToStringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.keyvalue.riak.DataStoreOperationException;
import org.springframework.data.keyvalue.riak.convert.KeyValueStoreMetaData;
import org.springframework.data.keyvalue.riak.mapreduce.MapReduceJob;
import org.springframework.data.keyvalue.riak.mapreduce.MapReduceOperations;
import org.springframework.data.keyvalue.riak.mapreduce.RiakMapReduceJob;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJacksonHttpMessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.*;
import org.springframework.web.client.support.RestGatewaySupport;

import javax.mail.BodyPart;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An implementation of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations}
 * and {@link org.springframework.data.keyvalue.riak.mapreduce.MapReduceOperations} for the Riak
 * data store.
 * <p/>
 * To use the RiakTemplate, create a singleton in your Spring application-context.xml:
 * <pre><code>
 * &lt;bean id="riak" class="org.springframework.data.keyvalue.riak.core.RiakTemplate"
 *     p:defaultUri="http://localhost:8098/riak/{bucket}/{key}"
 *     p:mapReduceUri="http://localhost:8098/mapred"/>
 * </code></pre>
 * To store and retrieve objects in Riak, use the setXXX and getXXX methods (example in
 * Groovy):
 * <pre><code>
 * def obj = new TestObject(name: "My Name", age: 40)
 * riak.set([bucket: "mybucket", key: "mykey"], obj)
 * ...
 * def name = riak.get([bucket: "mybucket", key: "mykey"]).name
 * println "Hello $name!"
 * </code></pre>
 * You're key object should be one of: <ul><li>A <code>String</code> encoding the bucket and key
 * together, separated by a colon. e.g. "mybucket:mykey"</li> <li>An implementation of
 * BucketKeyPair (like {@link org.springframework.data.keyvalue.riak.core.SimpleBucketKeyPair})</li>
 * <li>A <code>Map</code> with both a "bucket" and a "key" specified.</li> <li>A
 * <code>String</code> of only the key name, but specifying a bucket by using the {@link
 * org.springframework.data.keyvalue.riak.convert.KeyValueStoreMetaData} annotation on the
 * object you're storing.</li></ul>
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class RiakTemplate extends RestGatewaySupport implements KeyValueStoreOperations, MapReduceOperations, InitializingBean {

  /**
   * Client ID used by Riak to correlate updates.
   */
  private static final String RIAK_CLIENT_ID = "org.springframework.data.keyvalue.riak.core.RiakTemplate/1.0";
  /**
   * Regex used to extract host, port, and prefix from the given URI.
   */
  private static final Pattern prefix = Pattern.compile(
      "http[s]?://(\\S+):([0-9]+)/(\\S+)/\\{bucket\\}(\\S+)");
  /**
   * Do we need to handle Groovy strings in the Jackson JSON processor?
   */
  private static final boolean groovyPresent = ClassUtils.isPresent(
      "org.codehaus.groovy.runtime.GStringImpl",
      RiakTemplate.class.getClassLoader());
  /**
   * For getting a <code>java.util.Date</code> from the Last-Modified header.
   */
  private static SimpleDateFormat httpDate = new SimpleDateFormat(
      "EEE, d MMM yyyy HH:mm:ss z");

  protected final Logger log = LoggerFactory.getLogger(getClass());
  /**
   * For converting objects to/from other kinds of objects.
   */
  protected ConversionService conversionService = ConversionServiceFactory.createDefaultConversionService();
  /**
   * For caching objects based on ETags.
   */
  protected ConcurrentSkipListMap<BucketKeyPair, RiakValue<?>> cache = new ConcurrentSkipListMap<BucketKeyPair, RiakValue<?>>();
  /**
   * Whether or not to use the ETag-based cache.
   */
  protected boolean useCache = true;
  /**
   * {@link ExecutorService} to use for running asynchronous jobs.
   */
  protected ExecutorService executorService = Executors.newCachedThreadPool();
  /**
   * The URI to use inside the RestTemplate.
   */
  protected String defaultUri = "http://localhost:8098/riak/{bucket}/{key}";
  /**
   * The URI for the Riak Map/Reduce API.
   */
  protected String mapReduceUri = "http://localhost:8098/mapred";
  /**
   * A list of resolvers to turn a single object into a {@link BucketKeyPair}.
   */
  protected List<BucketKeyResolver> bucketKeyResolvers;
  /**
   * The default QosParameters to use for all operations through this template.
   */
  protected QosParameters defaultQosParameters = null;

  /**
   * Take all the defaults.
   */
  public RiakTemplate() {
    setRestTemplate(new RestTemplate());
  }

  /**
   * Use the specified {@link org.springframework.http.client.ClientHttpRequestFactory}.
   *
   * @param requestFactory
   */
  public RiakTemplate(ClientHttpRequestFactory requestFactory) {
    super(requestFactory);
    setRestTemplate(new RestTemplate());
  }

  /**
   * Use the specified defaultUri and mapReduceUri.
   *
   * @param defaultUri
   * @param mapReduceUri
   */
  public RiakTemplate(String defaultUri, String mapReduceUri) {
    setRestTemplate(new RestTemplate());
    this.setDefaultUri(defaultUri);
    this.mapReduceUri = mapReduceUri;
  }

  public ConversionService getConversionService() {
    return conversionService;
  }

  /**
   * Specify the conversion service to use.
   *
   * @param conversionService
   */
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

  /**
   * Set the list of BucketKeyResolvers to use.
   *
   * @param bucketKeyResolvers
   */
  public void setBucketKeyResolvers(List<BucketKeyResolver> bucketKeyResolvers) {
    this.bucketKeyResolvers = bucketKeyResolvers;
  }

  public boolean isUseCache() {
    return useCache;
  }

  public void setUseCache(boolean useCache) {
    this.useCache = useCache;
  }

  /**
   * Extract the prefix from the URI for use in creating links.
   *
   * @return
   */
  public String getPrefix() {
    Matcher m = prefix.matcher(defaultUri);
    if (m.matches()) {
      return "/" + m.group(3);
    }
    return "/riak";
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public void setExecutorService(ExecutorService executorService) {
    this.executorService = executorService;
  }
/*----------------- Set Operations -----------------*/

  public <K, V> KeyValueStoreOperations set(K key, V value) {
    return setWithMetaData(key, value, null);
  }

  public <K, V> KeyValueStoreOperations set(K key, V value, QosParameters qosParams) {
    return setWithMetaData(key, value, null, qosParams);
  }

  public <K> KeyValueStoreOperations setAsBytes(K key, byte[] value) {
    return setAsBytes(key, value, null);
  }

  public <K> KeyValueStoreOperations setAsBytes(K key, byte[] value, QosParameters qosParams) {
    Assert.notNull(key, "Can't store an object with a NULL key.");
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, value);
    // If I don't give a bucket name, since I don't have an object type, use 'bytes'
    String bucketName = (null != bucketKeyPair.getBucket() ? bucketKeyPair.getBucket()
        .toString() : "bytes");
    // Get a key name that may or may not include the QOS parameters.
    String keyName = (null != qosParams ? bucketKeyPair.getKey()
        .toString() + extractQosParameters(qosParams) : bucketKeyPair.getKey().toString());
    RestTemplate restTemplate = getRestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.set("X-Riak-ClientId", RIAK_CLIENT_ID);
    headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
    HttpEntity<byte[]> entity = new HttpEntity<byte[]>(value, headers);
    try {
      restTemplate.put(defaultUri, entity, bucketName, keyName);
      if (log.isDebugEnabled()) {
        log.debug(String.format("PUT byte[]: bucket=%s, key=%s",
            bucketKeyPair.getBucket(),
            bucketKeyPair.getKey()));
      }
    } catch (RestClientException e) {
      throw new DataStoreOperationException(e.getMessage(), e);
    }
    return this;
  }

  public <K, V> KeyValueStoreOperations setWithMetaData(K key, V value, Map<String, String> metaData, QosParameters qosParams) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, value);
    // Get a key name that may or may not include the QOS parameters.
    String keyName = (null != qosParams ? bucketKeyPair.getKey()
        .toString() + extractQosParameters(qosParams) : bucketKeyPair.getKey().toString());
    RestTemplate restTemplate = getRestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.set("X-Riak-ClientId", RIAK_CLIENT_ID);
    headers.setContentType(extractMediaType(value));
    if (null != metaData) {
      for (Map.Entry<String, String> entry : metaData.entrySet()) {
        headers.set(entry.getKey(), entry.getValue());
      }
    }
    HttpEntity<V> entity = new HttpEntity<V>(value, headers);
    try {
      restTemplate.put(defaultUri, entity, bucketKeyPair.getBucket(), keyName);
      if (log.isDebugEnabled()) {
        log.debug(String.format("PUT object: bucket=%s, key=%s, value=%s",
            bucketKeyPair.getBucket(),
            bucketKeyPair.getKey(),
            value));
      }
    } catch (RestClientException e) {
      throw new DataStoreOperationException(e.getMessage(), e);
    }
    return this;
  }

  public <K, V> KeyValueStoreOperations setWithMetaData(K key, V value, Map<String, String> metaData) {
    return setWithMetaData(key, value, metaData, null);
  }

  /*----------------- Get Operations -----------------*/

  public <K, T> RiakValue<T> getWithMetaData(K key, Class<T> requiredType) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    // If no bucket name is given, infer it from the type name.
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
      ResponseEntity<T> result = restTemplate.getForEntity(defaultUri,
          requiredType,
          bucketName,
          bucketKeyPair.getKey());
      if (result.hasBody()) {
        RiakMetaData meta = extractMetaData(result.getHeaders());
        RiakValue val = new RiakValue(result.getBody(), meta);
        if (useCache) {
          cache.put(bucketKeyPair, val);
        }
        return val;
      }
    } catch (HttpClientErrorException e) {
      if (e.getStatusCode() != HttpStatus.NOT_FOUND) {
        throw new DataStoreOperationException(e.getMessage(), e);
      }
    } catch (RestClientException rce) {
      // IGNORE
    } catch (EOFException eof) {
      // IGNORE
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    return null;
  }

  public <K, V> V get(K key) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    Class targetClass;
    try {
      // Since no type is specified, first try using the bucket name as the target class...
      targetClass = Class.forName(bucketKeyPair.getBucket().toString());
    } catch (Throwable ignored) {
      // ...if that doesn't work, just use a Map, which we know will work.
      targetClass = Map.class;
    }
    RiakValue<V> obj = getWithMetaData(bucketKeyPair, targetClass);
    return (null != obj ? obj.get() : null);
  }

  public <K> byte[] getAsBytes(K key) {
    RiakValue<byte[]> obj = getAsBytesWithMetaData(key);
    return (null != obj ? obj.get() : null);
  }

  public <K> RiakValue<byte[]> getAsBytesWithMetaData(K key) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    final RestTemplate restTemplate = getRestTemplate();
    if (log.isDebugEnabled()) {
      log.debug(String.format("GET object: bucket=%s, key=%s, type=byte[]",
          bucketKeyPair.getBucket(),
          bucketKeyPair.getKey()));
    }

    try {
      RiakValue<byte[]> bytes = (RiakValue<byte[]>) restTemplate.execute(
          defaultUri,
          HttpMethod.GET,
          new RequestCallback() {
            public void doWithRequest(ClientHttpRequest request) throws
                IOException {
              List<MediaType> mediaTypes = new ArrayList<MediaType>();
              mediaTypes.add(MediaType.APPLICATION_JSON);
              request.getHeaders().setAccept(mediaTypes);
            }
          },
          new ResponseExtractor<Object>() {
            public Object extractData(ClientHttpResponse response) throws
                IOException {
              InputStream in = response.getBody();
              ByteArrayOutputStream out = new ByteArrayOutputStream();
              byte[] buff = new byte[in.available()];
              for (int bytesRead = in.read(buff); bytesRead > 0; bytesRead = in.read(
                  buff)) {
                out.write(buff, 0, bytesRead);
              }

              HttpHeaders headers = response.getHeaders();
              RiakMetaData meta = extractMetaData(headers);
              RiakValue<byte[]> val = new RiakValue<byte[]>(out.toByteArray(),
                  meta);
              return val;
            }
          },
          bucketKeyPair.getBucket(),
          bucketKeyPair.getKey());
      if (useCache) {
        cache.put(bucketKeyPair, bytes);
      }
      return bytes;
    } catch (HttpClientErrorException e) {
      if (e.getStatusCode() != HttpStatus.NOT_FOUND) {
        throw new DataStoreOperationException(e.getMessage(), e);
      }
    } catch (RestClientException e) {
      throw new DataStoreOperationException(e.getMessage(), e);
    }
    return null;
  }

  public <K, T> T getAsType(K key, Class<T> requiredType) {
    if (useCache) {
      Object obj = checkCache(key, requiredType);
      if (null != obj) {
        return (T) obj;
      }
    }
    RiakValue<T> obj = getWithMetaData(key, requiredType);
    return (null != obj ? obj.get() : null);
  }

  public <K, V> V getAndSet(K key, V value) {
    V old = (V) getAsType(key, value.getClass());
    set(key, value);
    return old;
  }

  public <K> byte[] getAndSetAsBytes(K key, byte[] value) {
    byte[] old = getAsBytes(key);
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

  /*----------------- Only-Set-Once Operations -----------------*/

  public <K, V> KeyValueStoreOperations setIfKeyNonExistent(K key, V value) {
    if (!containsKey(key)) {
      set(key, value);
    } else {
      if (log.isDebugEnabled()) {
        log.debug(String.format("key: %s already exists. Not adding %s",
            key,
            value));
      }
    }
    return this;
  }

  public <K> KeyValueStoreOperations setIfKeyNonExistentAsBytes(K key, byte[] value) {
    if (!containsKey(key)) {
      setAsBytes(key, value);
    } else {
      if (log.isDebugEnabled()) {
        log.debug(String.format("key: %s already exists. Not adding %s",
            key,
            value));
      }
    }
    return this;
  }

  /*----------------- Multiple Item Operations -----------------*/

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

  /*----------------- Key Operations -----------------*/

  public <K> boolean containsKey(K key) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    RestTemplate restTemplate = getRestTemplate();
    HttpHeaders headers = null;
    try {
      headers = restTemplate.headForHeaders(defaultUri,
          bucketKeyPair.getBucket(),
          bucketKeyPair.getKey());
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
      //if (!stillExists) {
      //stillExists = containsKey(key);
      //}
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
    try {
      ResponseEntity<List> resp = restTemplate.postForEntity(mapReduceUri,
          job.toJson(),
          List.class);
      if (resp.hasBody()) {
        if (!targetType.isAssignableFrom(List.class)) {
          // M/R jobs always return a List. Try to turn the List into something else.
          List<?> results = (List<?>) resp.getBody();
          if (results.size() == 1) {
            // A List of size 1 get's returned as the object at list[0].
            Object obj = results.get(0);
            if (obj.getClass() != targetType) {
              // I can't just return it as-is, I have to convert it first.
              ConversionService conv = getConversionService();
              if (conv.canConvert(obj.getClass(), targetType)) {
                return conv.convert(obj, targetType);
              } else {
                throw new DataAccessResourceFailureException(
                    "Can't find a converter to to convert " + obj.getClass() + " returned from M/R job to required type " + targetType);
              }
            } else {
              return (T) obj;
            }
          }
        }
        return (T) resp.getBody();
      }
    } catch (RestClientException e) {
      throw new DataStoreOperationException(e.getMessage(), e);
    }
    return null;
  }

  public <T> Future<List<T>> submit(MapReduceJob job) {
    // Run this job asynchronously.
    return executorService.submit(job);
  }

  /*----------------- Link Operations -----------------*/

  /**
   * Use Riak's native Link mechanism to link two entries together.
   *
   * @param destination Key to the child object
   * @param source      Key to the parent object
   * @param tag         The tag for this relationship
   * @return This template interface
   */
  public <K1, K2> RiakTemplate link(K1 destination, K2 source, String tag) {
    BucketKeyPair bkpFrom = resolveBucketKeyPair(source, null);
    BucketKeyPair bkpTo = resolveBucketKeyPair(destination, null);
    RestTemplate restTemplate = getRestTemplate();

    // Skip all conversion on the data since all we care about is the Link header.
    RiakValue<byte[]> fromObj = getAsBytesWithMetaData(source);
    if (null == fromObj) {
      throw new DataStoreOperationException(
          "Cannot link from a non-existent source: " + source);
    }
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(fromObj.getMetaData().getContentType());
    Object linksObj = fromObj.getMetaData().getProperties().get("Link");
    List<String> links = new ArrayList<String>();
    // First add all existing links...
    if (linksObj instanceof List) {
      links.addAll((List) linksObj);
    } else if (linksObj instanceof String) {
      links.add(linksObj.toString());
    }
    // ...then add the link we're creating...
    links.add(String.format("<%s/%s/%s>; riaktag=\"%s\"",
        getPrefix(),
        bkpTo.getBucket(),
        bkpTo.getKey(),
        tag));
    String linkHeader = StringUtils.collectionToCommaDelimitedString(links);
    headers.set("Link", linkHeader);
    // Make sure to store the data back, otherwise it gets lost!
    // Basho at some point will likely add the ability to updated metadata separate
    // from the content. Until then, we have to transfer the body back-and-forth.
    HttpEntity entity = new HttpEntity(fromObj.get(), headers);
    restTemplate.put(defaultUri, entity, bkpFrom.getBucket(), bkpFrom.getKey());

    return this;
  }

  /**
   * Use Riak's link walking mechanism to retrieve a multipart message that will be decoded like
   * they were individual objects (e.g. using the built-in HttpMessageConverters of
   * RestTemplate).
   *
   * @param source
   * @param tag
   * @return
   */
  public <T, K> T linkWalk(K source, String tag) {
    BucketKeyPair bkpSource = resolveBucketKeyPair(source, null);
    final RestTemplate restTemplate = getRestTemplate();
    final List<MediaType> types = new ArrayList<MediaType>();
    types.add(MediaType.ALL);
    T returnObj = (T) restTemplate.execute(defaultUri + "/_,{tag},_",
        HttpMethod.GET,
        new RequestCallback() {
          public void doWithRequest(ClientHttpRequest request) throws
              IOException {
            // Make sure I can accept a multipart/mixed response.
            request.getHeaders().setAccept(types);
          }
        },
        new ResponseExtractor<Object>() {
          public Object extractData(ClientHttpResponse response) throws
              IOException {
            String contentType = ((List) response.getHeaders().get("Content-Type")).get(0)
                .toString();
            if (contentType.startsWith("multipart/mixed")) {
              List<Object> results = new LinkedList<Object>();
              ByteArrayDataSource ds = new ByteArrayDataSource(response.getBody(),
                  "multipart/mixed");
              try {
                // All this mess is for extracting multipart data from our response.
                // I'm using the javax.mail stuff because it's the best multipart library
                // that's in Maven and it's a common dependency anyway.
                MimeMultipart mp = new MimeMultipart(ds);
                int msgCnt = mp.getCount();
                for (int i = 0; i < msgCnt; i++) {
                  BodyPart bp = mp.getBodyPart(i);
                  if (bp.getContentType().startsWith("multipart/mixed")) {
                    MimeMultipart part = (MimeMultipart) bp.getContent();
                    int partCnt = part.getCount();
                    for (int j = 0; j < partCnt; j++) {
                      final BodyPart partBody = part.getBodyPart(j);
                      String partType = partBody.getContentType();
                      String link = partBody.getHeader("Link")[0];
                      String[] links = StringUtils.delimitedListToStringArray(link, ",");
                      String bucketName = null;
                      for (String s : links) {
                        if (s.contains("rel=\"up\"")) {
                          String[] linkParts = StringUtils.delimitedListToStringArray(s, ";");
                          int start = linkParts[0].lastIndexOf("/");
                          bucketName = linkParts[0].substring(start + 1,
                              linkParts[0].length() - 1);
                          break;
                        }
                      }
                      Class clazz = Map.class;
                      if (null != bucketName) {
                        try {
                          clazz = Class.forName(bucketName);
                        } catch (ClassNotFoundException e) {
                        }
                      }

                      // Can convert message?
                      for (HttpMessageConverter converter : restTemplate.getMessageConverters()) {
                        if (converter.canRead(clazz, MediaType.parseMediaType(partType))) {
                          HttpInputMessage msg = new HttpInputMessage() {
                            public InputStream getBody() throws IOException {
                              try {
                                return partBody.getInputStream();
                              } catch (MessagingException e) {
                                log.error(e.getMessage(), e);
                              }
                              return null;
                            }

                            public HttpHeaders getHeaders() {
                              return new HttpHeaders();
                            }
                          };
                          results.add(converter.read(clazz, msg));
                          break;
                        }
                      }

                      log.debug(String.format("results=%s", results));
                    }
                  }
                }
              } catch (MessagingException e) {
                log.error(e.getMessage(), e);
              }

              return results;
            }
            return null;
          }
        },
        bkpSource.getBucket(),
        bkpSource.getKey(),
        tag);
    return returnObj;
  }

  /*----------------- Bucket Operations -----------------*/

  public <B> Map<String, Object> getBucketSchema(B bucket) {
    return getBucketSchema(bucket, false);
  }

  public <B> Map<String, Object> getBucketSchema(B bucket, boolean listKeys) {
    RestTemplate restTemplate = getRestTemplate();
    ResponseEntity<Map> resp = restTemplate.getForEntity(defaultUri,
        Map.class,
        bucket,
        (listKeys ? "?keys=true" : ""));
    if (resp.hasBody()) {
      return resp.getBody();
    } else {
      throw new DataStoreOperationException(
          "Error encountered retrieving bucket schema (Status: " + resp.getStatusCode() + ")");
    }
  }

  public <B> KeyValueStoreOperations updateBucketSchema(B bucket, Map<String, Object> props) {
    Map<Object, Object> bucketProps = new LinkedHashMap<Object, Object>();
    bucketProps.put("props", props);
    RestTemplate restTemplate = getRestTemplate();
    String bucketName;
    if (bucket instanceof String) {
      bucketName = bucket.toString();
    } else {
      BucketKeyPair bkp = resolveBucketKeyPair(bucket, null);
      bucketName = bkp.getBucket().toString();
    }
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    HttpEntity entity = new HttpEntity(bucketProps, headers);
    restTemplate.put(defaultUri, entity, bucketName, "");
    return this;
  }

  public void afterPropertiesSet() throws Exception {
    Assert.notNull(conversionService,
        "Must specify a valid ConversionService.");
    if (null == bucketKeyResolvers) {
      bucketKeyResolvers = new ArrayList<BucketKeyResolver>();
      bucketKeyResolvers.add(new SimpleBucketKeyResolver());
    }

    List<HttpMessageConverter<?>> converters = getRestTemplate().getMessageConverters();
    ObjectMapper mapper = new ObjectMapper();
    CustomSerializerFactory fac = new CustomSerializerFactory();
    if (groovyPresent) {
      // Native conversion for Groovy GString objects
      fac.addSpecificMapping(GStringImpl.class, ToStringSerializer.instance);
    }
    mapper.setSerializerFactory(fac);
    for (HttpMessageConverter converter : converters) {
      if (converter instanceof MappingJacksonHttpMessageConverter) {
        ((MappingJacksonHttpMessageConverter) converter).setObjectMapper(
            mapper);
      }
    }
  }

  /*----------------- Utilities -----------------*/

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
      if (null == bucketKeyPair.getBucket() && null != val) {
        // No bucket specified, check for an annotation that specified bucket name.
        Annotation meta = (val instanceof Class ? (Class) val : val.getClass()).getAnnotation(
            KeyValueStoreMetaData.class);
        if (null != meta) {
          String bucket = ((KeyValueStoreMetaData) meta).bucket();
          if (null != bucket) {
            return new SimpleBucketKeyPair<String, Object>(bucket,
                bucketKeyPair.getKey());
          }
        }
      }
      return bucketKeyPair;
    }
    throw new DataStoreOperationException(String.format(
        "No resolvers available to resolve bucket/key pair from %s",
        key));
  }

  protected MediaType extractMediaType(Object value) {
    MediaType mediaType = (value instanceof byte[] ? MediaType.APPLICATION_OCTET_STREAM : MediaType.APPLICATION_JSON);
    if (value.getClass().getAnnotations().length > 0) {
      KeyValueStoreMetaData meta = value.getClass()
          .getAnnotation(KeyValueStoreMetaData.class);
      if (null != meta) {
        // Use the media type specified on the annotation.
        mediaType = MediaType.parseMediaType(meta.mediaType());
      }
    }
    return mediaType;
  }

  protected RiakMetaData extractMetaData(HttpHeaders headers) throws
      IOException {
    Map<String, Object> props = new LinkedHashMap<String, Object>();
    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      List<String> val = entry.getValue();
      Object prop = (1 == val.size() ? val.get(0) : val);
      try {
        if (entry.getKey().equals("Last-Modified") || entry.getKey()
            .equals("Date")) {
          prop = httpDate.parse(val.get(0));
        }
      } catch (ParseException e) {
        log.error(e.getMessage(), e);
      }

      if (entry.getKey().equals("Link")) {
        List<String> links = new ArrayList<String>();
        for (String link : entry.getValue()) {
          String[] parts = link.split(",");
          for (String part : parts) {
            String s = part.replaceAll("<(.+)>; rel=\"(\\S+)\"[,]?", "").trim();
            if (!"".equals(s)) {
              links.add(s);
            }
          }
        }
        props.put("Link", links);
      } else {
        props.put(entry.getKey().toString(), prop);
      }
    }
    props.put("ETag", headers.getETag());
    RiakMetaData meta = new RiakMetaData(headers.getContentType(), props);

    return meta;
  }

  protected <K, T> T checkCache(K key, Class<T> requiredType) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, requiredType);
    RiakValue<?> obj = cache.get(bucketKeyPair);
    if (null != obj) {
      String bucketName = (null != bucketKeyPair.getBucket() ? bucketKeyPair.getBucket()
          .toString() : requiredType.getName());
      RestTemplate restTemplate = getRestTemplate();
      HttpHeaders resp = restTemplate.headForHeaders(defaultUri,
          bucketName,
          bucketKeyPair.getKey());
      if (!obj.getMetaData()
          .getProperties()
          .get("ETag")
          .toString()
          .equals(resp.getETag())) {
        obj = null;
      } else {
        if (log.isDebugEnabled()) {
          log.debug("Returning CACHED object: " + obj);
        }
      }
    }

    if (null != obj && obj.getClass() == requiredType) {
      return (T) obj.get();
    } else {
      return null;
    }
  }

  /**
   * Get a string that represents the QOS parameters, taken either from the specified object or
   * from the template defaults.
   *
   * @param qosParams
   * @return
   */
  protected String extractQosParameters(QosParameters qosParams) {
    List<String> params = new LinkedList<String>();
    if (null != qosParams.getReadThreshold()) {
      params.add(String.format("r=%s", qosParams.<Object>getReadThreshold()));
    } else if (null != defaultQosParameters && null != defaultQosParameters.getReadThreshold()) {
      params.add(String.format("r=%s", defaultQosParameters.getReadThreshold()));
    }
    if (null != qosParams.getWriteThreshold()) {
      params.add(String.format("w=%s", qosParams.<Object>getWriteThreshold()));
    } else if (null != defaultQosParameters && null != defaultQosParameters.getWriteThreshold()) {
      params.add(String.format("w=%s", defaultQosParameters.getWriteThreshold()));
    }
    if (null != qosParams.getDurableWriteThreshold()) {
      params.add(String.format("dw=%s", qosParams.<Object>getDurableWriteThreshold()));
    } else if (null != defaultQosParameters && null != defaultQosParameters.getDurableWriteThreshold()) {
      params.add(String.format("dw=%s", defaultQosParameters.getDurableWriteThreshold()));
    }

    return (params.size() > 0 ? "?" + StringUtils.collectionToDelimitedString(
        params,
        "&") : "");
  }

}
