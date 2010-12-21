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

import org.springframework.core.convert.ConversionService;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.keyvalue.riak.DataStoreOperationException;
import org.springframework.data.keyvalue.riak.mapreduce.MapReduceJob;
import org.springframework.data.keyvalue.riak.mapreduce.MapReduceOperations;
import org.springframework.data.keyvalue.riak.mapreduce.RiakMapReduceJob;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.web.client.*;

import javax.mail.BodyPart;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;
import java.util.concurrent.Future;

/**
 * An implementation of {@link org.springframework.data.keyvalue.riak.core.BucketKeyValueStoreOperations}
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
 * riak.set("mybucket", "mykey", obj)
 * ...
 * def name = riak.get("mybucket", "mykey").name
 * println "Hello $name!"
 * </code></pre>
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakTemplate extends AbstractRiakTemplate implements BucketKeyValueStoreOperations, MapReduceOperations {

  /**
   * Take all the defaults.
   */
  public RiakTemplate() {
    super();
  }

  /**
   * Use the specified {@link org.springframework.http.client.ClientHttpRequestFactory}.
   *
   * @param requestFactory
   */
  public RiakTemplate(ClientHttpRequestFactory requestFactory) {
    super(requestFactory);
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

/*----------------- Set Operations -----------------*/

  public <B, K, V> BucketKeyValueStoreOperations set(B bucket, K key, V value) {
    return setWithMetaData(bucket, key, value, null, null);
  }

  public <B, K, V> BucketKeyValueStoreOperations set(B bucket, K key, V value, QosParameters qosParams) {
    return setWithMetaData(bucket, key, value, null, qosParams);
  }

  public <B, K> BucketKeyValueStoreOperations setAsBytes(B bucket, K key, byte[] value) {
    return setAsBytes(bucket, key, value, null);
  }

  public <B, K> BucketKeyValueStoreOperations setAsBytes(B bucket, K key, byte[] value, QosParameters qosParams) {
    return setWithMetaData(bucket, key, value, null, qosParams);
  }

  public <B, K, V> BucketKeyValueStoreOperations setWithMetaData(B bucket, K key, V value, Map<String, String> metaData, QosParameters qosParams) {
    Assert.notNull(key, "Key cannot be null!");
    // Get a key name that may or may not include the QOS parameters.
    String keyName = (null != qosParams ? key.toString() + extractQosParameters(qosParams) : key
        .toString());

    KeyValueStoreMetaData origMeta = getMetaData(bucket, keyName);
    String vclock = null;
    if (null != origMeta) {
      vclock = origMeta.getProperties().get(RIAK_VCLOCK).toString();
    }
    RestTemplate restTemplate = getRestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.set("X-Riak-ClientId", RIAK_CLIENT_ID);
    headers.setContentType(extractMediaType(value));
    if (null != vclock) {
      headers.set(RIAK_VCLOCK, vclock);
    }
    if (null != metaData) {
      for (Map.Entry<String, String> entry : metaData.entrySet()) {
        headers.set(entry.getKey(), entry.getValue());
      }
    }
    headers.set(RIAK_META_CLASSNAME, value.getClass().getName());
    HttpEntity<V> entity = new HttpEntity<V>(value, headers);
    try {
      restTemplate.put(defaultUri, entity, bucket, keyName);
      if (log.isDebugEnabled()) {
        log.debug(String.format("PUT object: bucket=%s, key=%s, value=%s", bucket, key, value));
      }
    } catch (RestClientException e) {
      throw new DataStoreOperationException(e.getMessage(), e);
    }
    return this;
  }

  public <B, K, V> BucketKeyValueStoreOperations setWithMetaData(B bucket, K key, V value, Map<String, String> metaData) {
    return setWithMetaData(bucket, key, value, metaData, null);
  }

  /*----------------- Put Operations -----------------*/

  /**
   * Save an object to Riak and let it generate an ID for it.
   *
   * @param bucket
   * @param value
   * @return The generated ID
   */
  public <B, V> String put(B bucket, V value) {
    return put(bucket, value, null);
  }

  /**
   * Save an object to Riak and let it generate an ID for it.
   *
   * @param bucket
   * @param value
   * @param metaData
   * @return The generated ID
   */
  public <B, V> String put(B bucket, V value, Map<String, String> metaData) {
    Assert.notNull(bucket, "Bucket cannot be null.");
    String bucketName = bucket.toString();
    RestTemplate restTemplate = getRestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.set("X-Riak-ClientId", RIAK_CLIENT_ID);
    headers.setContentType(extractMediaType(value));
    if (null != metaData) {
      for (Map.Entry<String, String> entry : metaData.entrySet()) {
        headers.set(entry.getKey(), entry.getValue());
      }
    }
    headers.set(RIAK_META_CLASSNAME, value.getClass().getName());
    HttpEntity<V> entity = new HttpEntity<V>(value, headers);
    try {
      URI uri = restTemplate.postForLocation(defaultUri, entity, bucketName, "");
      String suri = uri.toString();
      String id = suri.substring(suri.lastIndexOf("/") + 1);
      if (log.isDebugEnabled()) {
        log.debug("New ID: " + id);
      }
      return id;
    } catch (RestClientException e) {
      throw new DataStoreOperationException(e.getMessage(), e);
    }
  }

  /*----------------- Get Operations -----------------*/

  public <B, K> RiakMetaData getMetaData(B bucket, K key) {
    RestTemplate restTemplate = getRestTemplate();
    HttpHeaders headers;
    try {
      headers = restTemplate.headForHeaders(defaultUri, bucket, key);
      return extractMetaData(headers);
    } catch (ResourceAccessException e) {
    } catch (IOException e) {
      throw new DataAccessResourceFailureException(e.getMessage(), e);
    }
    return null;
  }

  @SuppressWarnings({"unchecked"})
  public <B, K, T> RiakValue<T> getWithMetaData(B bucket, K key, Class<T> requiredType) {
    // If no bucket name is given, infer it from the type name.
    String bucketName = (null != bucket ? bucket.toString() : requiredType.getName());
    RestTemplate restTemplate = getRestTemplate();
    if (log.isDebugEnabled()) {
      log.debug(String.format("GET object: bucket=%s, key=%s, type=%s",
          bucketName,
          key,
          requiredType.getName()));
    }
    Class<?> origType = getType(bucket, key);
    RiakValue<T> val = null;
    try {
      ResponseEntity<?> result = restTemplate.getForEntity(defaultUri,
          requiredType,
          bucketName,
          key);
      val = extractValue(result, requiredType, requiredType);
    } catch (HttpClientErrorException e) {
      switch (e.getStatusCode()) {
        case NOT_ACCEPTABLE:
          // Can't convert using HttpMessageConverter. Try fetching as the original type
          // and using the conversion service to convert.
          ResponseEntity<?> result = restTemplate.getForEntity(defaultUri,
              origType,
              bucketName,
              key);
          try {
            val = extractValue(result, origType, requiredType);
          } catch (IOException ioe) {
            throw new DataStoreOperationException(ioe.getMessage(), ioe);
          }
          break;
        case NOT_FOUND:
          // IGNORED
          break;
        default:
          throw new DataStoreOperationException(e.getMessage(), e);
      }
      if (e.getStatusCode() != HttpStatus.NOT_FOUND) {
        throw new DataStoreOperationException(e.getMessage(), e);
      }
    } catch (RestClientException rce) {
      if (rce.getMessage().contains("HTTP response code: 406")) {
        // Can't convert using HttpMessageConverter. Try fetching as the original type
        // and using the conversion service to convert.
        ResponseEntity<?> result = restTemplate.getForEntity(defaultUri,
            origType,
            bucketName,
            key);
        try {
          val = extractValue(result, origType, requiredType);
        } catch (IOException ioe) {
          throw new DataStoreOperationException(rce.getMessage(), rce);
        }
      } else {
        // IGNORE
        if (log.isDebugEnabled()) {
          log.debug("RestClientException: " + rce.getMessage());
        }
      }
    } catch (EOFException eof) {
      // IGNORE
      if (log.isDebugEnabled()) {
        log.debug("EOFException: " + eof.getMessage(), eof);
      }
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }

    if (null != val && useCache) {
      cache.put(new SimpleBucketKeyPair<Object, Object>(bucket, key), val);
    }
    return val;
  }

  @SuppressWarnings({"unchecked"})
  public <B, K, T> T get(B bucket, K key) {
    Class targetClass = getType(bucket, key);
    RiakValue<T> obj = getWithMetaData(bucket, key, targetClass);
    return (null != obj ? obj.get() : null);
  }

  public <B, K> byte[] getAsBytes(B bucket, K key) {
    RiakValue<byte[]> obj = getAsBytesWithMetaData(bucket, key);
    return (null != obj ? obj.get() : null);
  }

  @SuppressWarnings({"unchecked"})
  public <B, K> RiakValue<byte[]> getAsBytesWithMetaData(B bucket, K key) {
    final RestTemplate restTemplate = getRestTemplate();
    if (log.isDebugEnabled()) {
      log.debug(String.format("GET object: bucket=%s, key=%s, type=byte[]",
          bucket,
          key));
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
              mediaTypes.add(MediaType.APPLICATION_OCTET_STREAM);
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
          bucket,
          key);
      if (useCache) {
        cache.put(new SimpleBucketKeyPair(bucket, key), bytes);
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

  @SuppressWarnings({"unchecked"})
  public <B, K, T> T getAsType(B bucket, K key, Class<T> requiredType) {
    if (useCache) {
      Object obj = checkCache(new SimpleBucketKeyPair(bucket, key), requiredType);
      if (null != obj) {
        return (T) obj;
      }
    }
    RiakValue<T> obj = getWithMetaData(bucket, key, requiredType);
    return (null != obj ? obj.get() : null);
  }

  @SuppressWarnings({"unchecked"})
  public <B, K, V> V getAndSet(B bucket, K key, V value) {
    V old = (V) getAsType(bucket, key, value.getClass());
    set(bucket, key, value, null);
    return old;
  }

  public <B, K> byte[] getAndSetAsBytes(B bucket, K key, byte[] value) {
    byte[] old = getAsBytes(bucket, key);
    setAsBytes(bucket, key, value);
    return old;
  }

  public <B, K, V, T> T getAndSetAsType(B bucket, K key, V value, Class<T> requiredType) {
    T old = getAsType(bucket, key, requiredType);
    set(bucket, key, value);
    return old;
  }

  @SuppressWarnings({"unchecked"})
  public <K, V> List<V> getValues(List<K> keys) {
    List<V> results = new ArrayList<V>();
    for (K key : keys) {
      BucketKeyPair bkp = resolveBucketKeyPair(key, null);
      results.add((V) get(bkp.getBucket(), bkp.getKey()));
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
      results.add(getAsType(bkp.getBucket(), bkp.getKey(), requiredType));
    }
    return results;
  }

  public <T, K> List<T> getValuesAsType(Class<T> requiredType, K... keys) {
    List<K> keyList = new ArrayList<K>(keys.length);
    return getValuesAsType(keyList, requiredType);
  }

  /*----------------- Only-Set-Once Operations -----------------*/

  public <B, K, V> BucketKeyValueStoreOperations setIfKeyNonExistent(B bucket, K key, V value) {
    if (!containsKey(bucket, key)) {
      set(bucket, key, value);
    } else {
      if (log.isDebugEnabled()) {
        log.debug(String.format("key: %s already exists. Not adding %s",
            key,
            value));
      }
    }
    return this;
  }

  public <B, K> BucketKeyValueStoreOperations setIfKeyNonExistentAsBytes(B bucket, K key, byte[] value) {
    if (!containsKey(bucket, key)) {
      setAsBytes(bucket, key, value);
    } else {
      if (log.isDebugEnabled()) {
        log.debug(String.format("key: %s already exists. Not adding %s",
            key,
            value));
      }
    }
    return this;
  }

  /*----------------- Key Operations -----------------*/

  public <B, K> boolean containsKey(B bucket, K key) {
    RestTemplate restTemplate = getRestTemplate();
    HttpHeaders headers = null;
    try {
      headers = restTemplate.headForHeaders(defaultUri, bucket, key);
    } catch (ResourceAccessException e) {
    }
    return (null != headers);
  }

  @SuppressWarnings({"unchecked"})
  public <B, K> boolean delete(B bucket, K key) {
    return deleteKeys(new SimpleBucketKeyPair(bucket, key));
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

  @SuppressWarnings({"unchecked"})
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

  @SuppressWarnings({"unchecked"})
  public <T> Future<List<T>> submit(MapReduceJob job) {
    // Run this job asynchronously.
    return executorService.submit(job);
  }

  /*----------------- Link Operations -----------------*/

  /**
   * Use Riak's native Link mechanism to link two entries together.
   *
   * @param destBucket   Bucket of child entry
   * @param destKey      Key of child entry
   * @param sourceBucket Bucket of parent entry
   * @param sourceKey    Key of parent entry
   * @param tag          Tag for this relationship
   * @return
   */
  @SuppressWarnings({"unchecked"})
  public <B1, K1, B2, K2> RiakTemplate link(B1 destBucket, K1 destKey, B2 sourceBucket, K2 sourceKey, String tag) {
    RestTemplate restTemplate = getRestTemplate();

    // Skip all conversion on the data since all we care about is the Link header.
    RiakValue<byte[]> fromObj = getAsBytesWithMetaData(sourceBucket, sourceKey);
    if (null == fromObj) {
      throw new DataStoreOperationException(
          "Cannot link from a non-existent source: " + sourceBucket + ":" + sourceKey);
    }
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(fromObj.getMetaData().getContentType());
    headers.set(RIAK_VCLOCK, fromObj.getMetaData().getProperties().get(RIAK_VCLOCK).toString());
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
        destBucket,
        destKey,
        tag));
    String linkHeader = StringUtils.collectionToCommaDelimitedString(links);
    headers.set("Link", linkHeader);
    // Make sure to store the data back, otherwise it gets lost!
    // Basho at some point will likely add the ability to updated metadata separate
    // from the content. Until then, we have to transfer the body back-and-forth.
    HttpEntity entity = new HttpEntity(fromObj.get(), headers);
    restTemplate.put(defaultUri, entity, sourceBucket, sourceKey);

    return this;
  }

  /**
   * Use Riak's link walking mechanism to retrieve a multipart message that will be decoded like
   * they were individual objects (e.g. using the built-in HttpMessageConverters of
   * RestTemplate).
   *
   * @param bucket
   * @param key
   * @param tag
   * @return
   */
  @SuppressWarnings({"unchecked"})
  public <B, T, K> T linkWalk(B bucket, K key, String tag) {
    return (T) linkWalkAsType(bucket, key, tag, null);
  }

  /**
   * Use Riak's link walking mechanism to retrieve a multipart message that will be decoded like
   * they were individual objects (e.g. using the built-in HttpMessageConverters of
   * RestTemplate) and return the result as a list of objects of one of: <ol> <li>The type
   * specified by <code>requiredType</code></li> <li>If that's null, try using the bucket name
   * in which the object was stored</li> <li>If all else fails, use a {@link java.util.Map}</li>
   * </ol>
   *
   * @param bucket
   * @param key
   * @param tag
   * @param requiredType
   * @return
   */
  @SuppressWarnings({"unchecked"})
  public <B, T, K> T linkWalkAsType(B bucket, K key, String tag, final Class<T> requiredType) {
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
          @SuppressWarnings({"unchecked"})
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
                      String location = partBody.getHeader("Location")[0];
                      String key = location.substring(location.lastIndexOf("/") + 1);
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
                      Class<?> clazz = requiredType;
                      if (null == clazz) {
                        clazz = getType(bucketName, key);
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

                      if (log.isDebugEnabled()) {
                        log.debug(String.format("results=%s", results));
                      }
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
        bucket,
        key,
        tag);
    return returnObj;
  }

  /*----------------- Bucket Operations -----------------*/

  public <B> Map<String, Object> getBucketSchema(B bucket) {
    return getBucketSchema(bucket, false);
  }

  @SuppressWarnings({"unchecked"})
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

  @SuppressWarnings({"unchecked"})
  public <B> BucketKeyValueStoreOperations updateBucketSchema(B bucket, Map<String, Object> props) {
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

}
