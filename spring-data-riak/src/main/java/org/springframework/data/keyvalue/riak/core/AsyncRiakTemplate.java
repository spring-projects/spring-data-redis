/*
 * Copyright (c) 2010 by J. Brisbin <jon@jbrisbin.com>
 * Portions (c) 2010 by NPC International, Inc. or the
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

package org.springframework.data.keyvalue.riak.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.keyvalue.riak.DataStoreOperationException;
import org.springframework.data.keyvalue.riak.mapreduce.AsyncMapReduceOperations;
import org.springframework.data.keyvalue.riak.mapreduce.MapReduceJob;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.util.Assert;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class AsyncRiakTemplate extends AbstractRiakTemplate implements AsyncBucketKeyValueStoreOperations, AsyncMapReduceOperations {

  protected final Logger log = LoggerFactory.getLogger(getClass());

  protected ExecutorService workerPool = Executors.newCachedThreadPool();
  protected AsyncKeyValueStoreOperation<Throwable, Object> defaultErrorHandler = new LoggingErrorHandler();

  public AsyncRiakTemplate() {
    super();
  }

  public AsyncRiakTemplate(ClientHttpRequestFactory requestFactory) {
    super(requestFactory);
  }

  public ExecutorService getWorkerPool() {
    return workerPool;
  }

  public void setWorkerPool(ExecutorService workerPool) {
    this.workerPool = workerPool;
  }

  public AsyncKeyValueStoreOperation<Throwable, Object> getDefaultErrorHandler() {
    return defaultErrorHandler;
  }

  public void setDefaultErrorHandler(AsyncKeyValueStoreOperation<Throwable, Object> defaultErrorHandler) {
    this.defaultErrorHandler = defaultErrorHandler;
  }

  public <B, K, V, R> Future<?> set(B bucket, K key, V value, AsyncKeyValueStoreOperation<V, R> callback) {
    return setWithMetaData(bucket, key, value, null, null, callback);
  }

  public <B, K, V, R> Future<?> set(B bucket, K key, V value, QosParameters qosParams, AsyncKeyValueStoreOperation<V, R> callback) {
    return setWithMetaData(bucket, key, value, null, qosParams, callback);
  }

  public <B, K, R> Future<?> setAsBytes(B bucket, K key, byte[] value, AsyncKeyValueStoreOperation<byte[], R> callback) {
    return setWithMetaData(bucket, key, value, null, null, callback);
  }

  @SuppressWarnings({"unchecked"})
  public <B, K, V, R> Future<V> setWithMetaData(B bucket, K key, V value, Map<String, String> metaData, QosParameters qosParams, AsyncKeyValueStoreOperation<V, R> callback) {
    String bucketName = (null != bucket ? bucket.toString() : value.getClass().getName());
    // Get a key name that may or may not include the QOS parameters.
    Assert.notNull(key, "Cannot use a <NULL> key.");
    String keyName = (null != qosParams ? key.toString() + extractQosParameters(qosParams) : key
        .toString());

    KeyValueStoreMetaData origMeta = getMetaData(bucket, keyName);
    String vclock = null;
    if (null != origMeta) {
      vclock = origMeta.getProperties().get(RIAK_VCLOCK).toString();
    }

    HttpHeaders headers = defaultHeaders(metaData);
    headers.setContentType(extractMediaType(value));
    if (null != vclock) {
      headers.set(RIAK_VCLOCK, vclock);
    }
    headers.set(RIAK_META_CLASSNAME, value.getClass().getName());
    HttpEntity<V> entity = new HttpEntity<V>(value, headers);
    return (Future<V>) workerPool.submit(new AsyncPost<V, R>(bucketName,
        keyName,
        entity,
        callback));
  }

  public <B, V, R> Future<V> put(B bucket, V value, AsyncKeyValueStoreOperation<V, R> callback) {
    return put(bucket, value, null, null, callback);
  }

  public <B, V, R> Future<V> put(B bucket, V value, Map<String, String> metaData, AsyncKeyValueStoreOperation<V, R> callback) {
    return put(bucket, value, metaData, null, callback);
  }

  @SuppressWarnings({"unchecked"})
  public <B, V, R> Future<V> put(B bucket, V value, Map<String, String> metaData, QosParameters qosParams, AsyncKeyValueStoreOperation<V, R> callback) {
    Assert.notNull(bucket, "Bucket cannot be null");
    String bucketName = (null != qosParams ? bucket.toString() + extractQosParameters(qosParams) : bucket
        .toString());

    HttpHeaders headers = defaultHeaders(metaData);
    headers.setContentType(extractMediaType(value));
    headers.set(RIAK_META_CLASSNAME, value.getClass().getName());
    HttpEntity<V> entity = new HttpEntity<V>(value, headers);
    return (Future<V>) workerPool.submit(new AsyncPut<V, R>(bucketName, entity, callback));
  }

  public <B, K, V, R> Future<?> get(B bucket, K key, AsyncKeyValueStoreOperation<V, R> callback) {
    return getWithMetaData(bucket, key, null, callback);
  }

  public <B, K> RiakMetaData getMetaData(B bucket, K key) {
    RestTemplate restTemplate = getRestTemplate();
    HttpHeaders headers;
    try {
      headers = restTemplate.headForHeaders(defaultUri, bucket, key);
      RiakMetaData meta = extractMetaData(headers);
      meta.setBucket((null != bucket ? bucket.toString() : null));
      meta.setKey((null != key ? key.toString() : null));
      return meta;
    } catch (ResourceAccessException e) {
    } catch (IOException e) {
      throw new DataAccessResourceFailureException(e.getMessage(), e);
    }
    return null;
  }

  @SuppressWarnings({"unchecked"})
  public <B, R> Future<?> getBucketSchema(B bucket, QosParameters qosParams, final AsyncKeyValueStoreOperation<Map<String, Object>, R> callback) {
    Assert.notNull(bucket, "Bucket cannot be null");
    Assert.notNull(callback, "Callback cannot be null");

    String bucketName = (null != qosParams ? bucket.toString() + extractQosParameters(qosParams) : bucket
        .toString());

    return workerPool.submit(new AsyncGet(bucketName,
        "?keys=true",
        Map.class,
        new AsyncKeyValueStoreOperation<Object, Object>() {
          @SuppressWarnings({"unchecked"})
          public Object completed(KeyValueStoreMetaData meta, Object result) {
            return callback.completed(meta, (Map<String, Object>) result);
          }

          public Object failed(Throwable error) {
            return callback.failed(error);
          }
        }));
  }

  @SuppressWarnings({"unchecked"})
  public <B, K, T, R> Future<?> getWithMetaData(B bucket, K key, Class<T> requiredType, AsyncKeyValueStoreOperation<T, R> callback) {
    String bucketName = (null != bucket ? bucket.toString() : requiredType.getName());
    // Get a key name that may or may not include the QOS parameters.
    Assert.notNull(key, "Cannot use a null key.");
    Assert.notNull(callback, "Callback cannot be null");

    if (null == requiredType) {
      requiredType = (Class<T>) getType(bucketName, key.toString());
    }
    return workerPool.submit(new AsyncGet<T, R>(bucketName,
        key.toString(),
        requiredType,
        callback));
  }

  public <B, K, R> Future<?> getAsBytes(B bucket, K key, AsyncKeyValueStoreOperation<byte[], R> callback) {
    return getWithMetaData(bucket, key, byte[].class, callback);
  }

  public <B, K, T, R> Future<?> getAsType(B bucket, K key, Class<T> requiredType, AsyncKeyValueStoreOperation<T, R> callback) {
    return getWithMetaData(bucket, key, requiredType, callback);
  }

  public <B, K, V, R> Future<?> getAndSet(final B bucket, final K key, final V value, final AsyncKeyValueStoreOperation<V, R> callback) {
    final List<Future<?>> futures = new ArrayList<Future<?>>();
    try {
      getWithMetaData(bucket, key, null, new AsyncKeyValueStoreOperation<Object, Object>() {
        @SuppressWarnings({"unchecked"})
        public Object completed(KeyValueStoreMetaData meta, Object result) {
          futures.add(setWithMetaData(bucket, key, value, null, null, null));
          return callback.completed(meta, (V) result);
        }

        public Object failed(Throwable error) {
          return callback.failed(error);
        }
      }).get();
    } catch (InterruptedException e) {
      log.error(e.getMessage(), e);
    } catch (ExecutionException e) {
      log.error(e.getMessage(), e);
    }
    return futures.size() > 0 ? futures.get(0) : null;
  }

  public <B, K, R> Future<?> getAndSetAsBytes(B bucket, K key, byte[] value, AsyncKeyValueStoreOperation<byte[], R> callback) {
    return getAndSet(bucket, key, value, callback);
  }

  public <B, K, V, T, R> Future<?> getAndSetAsType(final B bucket, final K key, final V value, final Class<T> requiredType, final AsyncKeyValueStoreOperation<T, R> callback) {
    final List<Future<?>> futures = new ArrayList<Future<?>>();
    getWithMetaData(bucket, key, requiredType, new AsyncKeyValueStoreOperation<T, R>() {
      @SuppressWarnings({"unchecked"})
      public R completed(KeyValueStoreMetaData meta, T result) {
        try {
          setWithMetaData(bucket, key, value, null, null, null).get();
          return callback.completed(meta, result);
        } catch (InterruptedException e) {
          return callback.failed(e);
        } catch (ExecutionException e) {
          return callback.failed(e);
        }
      }

      public R failed(Throwable error) {
        return callback.failed(error);
      }
    });
    return futures.size() > 0 ? futures.get(0) : null;
  }

  public <B, K, V, R> Future<?> setIfKeyNonExistent(final B bucket, final K key, final V value, final AsyncKeyValueStoreOperation<V, R> callback) {
    return containsKey(bucket, key, new AsyncKeyValueStoreOperation<Boolean, Object>() {
      public Object completed(KeyValueStoreMetaData meta, Boolean result) {
        if (!result) {
          return setWithMetaData(bucket, key, value, null, null, callback);
        } else {
          return null;
        }
      }

      public Object failed(Throwable error) {
        return callback.failed(error);
      }
    });
  }

  public <B, K, R> Future<?> setIfKeyNonExistentAsBytes(final B bucket, final K key, final byte[] value, final AsyncKeyValueStoreOperation<byte[], R> callback) {
    return containsKey(bucket, key, new AsyncKeyValueStoreOperation<Boolean, Object>() {
      public Object completed(KeyValueStoreMetaData meta, Boolean result) {
        if (!result) {
          return setWithMetaData(bucket, key, value, null, null, callback);
        } else {
          return null;
        }
      }

      public Object failed(Throwable error) {
        return callback.failed(error);
      }
    });
  }

  public <B, K, R> Future<?> containsKey(B bucket, K key, final AsyncKeyValueStoreOperation<Boolean, R> callback) {
    Assert.notNull(bucket, "Bucket cannot be null when checking for existence.");
    Assert.notNull(key, "Key cannot be null when checking for existence");
    return workerPool.submit(new AsyncHead(bucket.toString(),
        key.toString(),
        new AsyncKeyValueStoreOperation<HttpHeaders, Object>() {
          public Object completed(KeyValueStoreMetaData meta, HttpHeaders result) {
            return callback.completed(null, (null != result));
          }

          public Object failed(Throwable error) {
            return callback.failed(error);
          }
        }));
  }

  public <B, K, R> Future<?> delete(B bucket, K key, AsyncKeyValueStoreOperation<Boolean, R> callback) {
    Assert.notNull(bucket, "Bucket cannot be null when deleting.");
    Assert.notNull(key, "Key cannot be null when deleting.");
    return workerPool.submit(new AsyncDelete(bucket.toString(), key.toString(), callback));
  }

  public <B, K, R> Future<?> setAsBytes(B bucket, K key, byte[] value, QosParameters qosParams, AsyncKeyValueStoreOperation<byte[], R> callback) {
    return setWithMetaData(bucket, key, value, null, qosParams, callback);
  }

  public <B, K, V, R> Future<?> setWithMetaData(B bucket, K key, V value, Map<String, String> metaData, AsyncKeyValueStoreOperation<V, R> callback) {
    return setWithMetaData(bucket, key, value, metaData, null, callback);
  }

  /* ---------------- Map/Reduce ---------------- */

  @SuppressWarnings({"unchecked"})
  public <R> Future<?> execute(MapReduceJob job, AsyncKeyValueStoreOperation<List<?>, R> callback) {
    HttpHeaders headers = defaultHeaders(null);
    headers.setContentType(MediaType.APPLICATION_JSON);
    HttpEntity<String> json = new HttpEntity<String>(job.toJson(), headers);
    return workerPool.submit(new AsyncMapReduce(json, callback));
  }

  /* ---------------- Runnable helpers ---------------- */
  protected class AsyncPut<V, R> implements Callable {

    private String bucket;
    private HttpEntity<V> entity = null;
    private AsyncKeyValueStoreOperation<V, R> callback = null;

    public AsyncPut(String bucket, HttpEntity<V> entity, AsyncKeyValueStoreOperation<V, R> callback) {
      this.bucket = bucket;
      this.entity = entity;
      this.callback = callback;
    }

    public R call() throws Exception {
      try {
        URI location = getRestTemplate().postForLocation(defaultUri, entity, bucket, "");
        String path = location.getPath();
        String key = path.substring(path.lastIndexOf("/") + 1);

        HttpHeaders headers = getRestTemplate().headForHeaders(defaultUri, bucket, key);
        if (null != callback) {
          RiakMetaData meta = extractMetaData(headers);
          meta.setBucket((null != bucket ? bucket.toString() : null));
          meta.setKey((null != key ? key.toString() : null));
          return callback.completed(meta, entity.getBody());
        }
      } catch (Throwable t) {
        DataStoreOperationException dsoe = new DataStoreOperationException(t.getMessage(), t);
        if (null != callback) {
          return callback.failed(dsoe);
        } else {
          defaultErrorHandler.failed(dsoe);
        }
      }
      return null;
    }

  }

  protected class AsyncPost<V, R> implements Callable {

    private String bucket;
    private String key;
    private HttpEntity<V> entity = null;
    private AsyncKeyValueStoreOperation<V, R> callback = null;

    public AsyncPost(String bucket, String key, HttpEntity<V> entity, AsyncKeyValueStoreOperation<V, R> callback) {
      this.bucket = bucket;
      this.key = key;
      this.entity = entity;
      this.callback = callback;
    }

    @SuppressWarnings({"unchecked"})
    public R call() throws Exception {
      try {
        HttpEntity<?> result = getRestTemplate().postForEntity(defaultUri,
            entity,
            (entity.getBody() instanceof byte[] ? byte[].class : entity.getBody().getClass()),
            bucket,
            key + "?returnbody=true");
        if (log.isDebugEnabled()) {
          log.debug(String.format("PUT object: bucket=%s, key=%s, value=%s",
              bucket,
              key,
              entity));
        }
        if (null != callback) {
          RiakMetaData meta = extractMetaData(result.getHeaders());
          meta.setBucket((null != bucket ? bucket.toString() : null));
          meta.setKey((null != key ? key.toString() : null));
          return callback.completed(meta, (V) result.getBody());
        }
      } catch (Throwable t) {
        DataStoreOperationException dsoe = new DataStoreOperationException(t.getMessage(), t);
        if (null != callback) {
          return callback.failed(dsoe);
        } else {
          defaultErrorHandler.failed(dsoe);
        }
      }
      return null;
    }

  }

  protected class AsyncMapReduce<R> implements Callable {

    private HttpEntity<String> entity = null;
    private AsyncKeyValueStoreOperation<List<?>, R> callback = null;

    public AsyncMapReduce(HttpEntity<String> entity, AsyncKeyValueStoreOperation<List<?>, R> callback) {
      this.entity = entity;
      this.callback = callback;
    }

    @SuppressWarnings({"unchecked"})
    public R call() throws Exception {
      try {
        HttpEntity<List> result = getRestTemplate().postForEntity(mapReduceUri,
            entity,
            List.class);
        if (log.isDebugEnabled()) {
          log.debug(String.format("M/R: json=%s", entity.getBody()));
        }
        if (null != callback) {
          RiakMetaData meta = extractMetaData(result.getHeaders());
          return callback.completed(meta, result.getBody());
        }
      } catch (Throwable t) {
        DataStoreOperationException dsoe = new DataStoreOperationException(t.getMessage(), t);
        if (null != callback) {
          return callback.failed(dsoe);
        } else {
          defaultErrorHandler.failed(dsoe);
        }
      }
      return null;
    }

  }

  protected class AsyncGet<T, R> implements Callable {

    private String bucket;
    private String key;
    private Class<T> requiredType;
    private AsyncKeyValueStoreOperation<T, R> callback = null;

    public AsyncGet(String bucket, String key, Class<T> requiredType, AsyncKeyValueStoreOperation<T, R> callback) {
      this.bucket = bucket;
      this.key = key;
      this.requiredType = requiredType;
      this.callback = callback;
    }

    public R call() throws Exception {
      try {
        ResponseEntity<T> result = getRestTemplate().getForEntity(defaultUri,
            requiredType,
            bucket,
            key);
        if (result.hasBody()) {
          RiakMetaData meta = extractMetaData(result.getHeaders());
          meta.setBucket((null != bucket ? bucket.toString() : null));
          meta.setKey((null != key ? key.toString() : null));
          RiakValue<T> val = new RiakValue<T>(result.getBody(), meta);
          if (useCache) {
            cache.put(new SimpleBucketKeyPair<Object, Object>(bucket, key), val);
          }
          if (null != callback) {
            return callback.completed(meta, val.get());
          }
          if (log.isDebugEnabled()) {
            log.debug(String.format("GET object: bucket=%s, key=%s, type=%s",
                bucket,
                key,
                requiredType.getName()));
          }
        }
      } catch (Throwable t) {
        DataStoreOperationException dsoe = new DataStoreOperationException(t.getMessage(), t);
        if (null != callback) {
          return callback.failed(dsoe);
        } else {
          defaultErrorHandler.failed(dsoe);
        }
      }
      return null;
    }
  }

  protected class AsyncHead<V, R> implements Callable {

    private String bucket;
    private String key;
    private AsyncKeyValueStoreOperation<HttpHeaders, R> callback = null;

    public AsyncHead(String bucket, String key, AsyncKeyValueStoreOperation<HttpHeaders, R> callback) {
      this.bucket = bucket;
      this.key = key;
      this.callback = callback;
    }

    public R call() throws Exception {
      try {
        HttpHeaders headers = getRestTemplate().headForHeaders(defaultUri, bucket, key);
        if (null != headers) {
          if (null != callback) {
            return callback.completed(null, headers);
          }
        }
      } catch (Throwable t) {
        DataStoreOperationException dsoe = new DataStoreOperationException(t.getMessage(), t);
        if (null != callback) {
          return callback.failed(dsoe);
        } else {
          defaultErrorHandler.failed(dsoe);
        }
      }
      return null;
    }
  }

  protected class AsyncDelete<V, R> implements Callable {

    private String bucket;
    private String key;
    private AsyncKeyValueStoreOperation<Boolean, R> callback = null;

    public AsyncDelete(String bucket, String key, AsyncKeyValueStoreOperation<Boolean, R> callback) {
      this.bucket = bucket;
      this.key = key;
      this.callback = callback;
    }

    public R call() throws Exception {
      try {
        getRestTemplate().delete(defaultUri, bucket, key);
        if (null != callback) {
          return callback.completed(null, true);
        }
      } catch (Throwable t) {
        DataStoreOperationException dsoe = new DataStoreOperationException(t.getMessage(), t);
        if (null != callback) {
          return callback.failed(dsoe);
        } else {
          defaultErrorHandler.failed(dsoe);
        }
      }
      return null;
    }
  }

  protected class LoggingErrorHandler implements AsyncKeyValueStoreOperation<Throwable, Object> {
    public Object completed(KeyValueStoreMetaData meta, Throwable result) {
      return null;
    }

    public Object failed(Throwable error) {
      log.error(error.getMessage(), error);
      return null;
    }
  }

}
