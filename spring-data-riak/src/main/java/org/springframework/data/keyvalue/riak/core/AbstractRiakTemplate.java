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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.groovy.runtime.GStringImpl;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ser.CustomSerializerFactory;
import org.codehaus.jackson.map.ser.ToStringSerializer;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.data.keyvalue.riak.DataStoreOperationException;
import org.springframework.data.keyvalue.riak.convert.KeyValueStoreMetaData;
import org.springframework.data.keyvalue.riak.util.Ignore404sErrorHandler;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJacksonHttpMessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.support.RestGatewaySupport;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base class for RiakTemplates that defines basic behaviour common to both kinds of templates
 * (Key/Value and Bucket/Key/Value).
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public abstract class AbstractRiakTemplate extends RestGatewaySupport implements InitializingBean, BeanClassLoaderAware {

  protected static final String RIAK_META_CLASSNAME = "X-Riak-Meta-ClassName";
  protected static final String RIAK_VCLOCK = "X-Riak-Vclock";

  /**
   * Regex used to extract host, port, and prefix from the given URI.
   */
  protected static final Pattern prefix = Pattern.compile(
      "http[s]?://(\\S+):([0-9]+)/(\\S+)/\\{bucket\\}(\\S+)");
  /**
   * Do we need to handle Groovy strings in the Jackson JSON processor?
   */
  protected final boolean groovyPresent = ClassUtils.isPresent(
      "org.codehaus.groovy.runtime.GStringImpl",
      getClass().getClassLoader());
  /**
   * For getting a <code>java.util.Date</code> from the Last-Modified header.
   */
  protected static SimpleDateFormat httpDate = new SimpleDateFormat(
      "EEE, d MMM yyyy HH:mm:ss z");

  protected final Log log = LogFactory.getLog(getClass());

  /**
   * Client ID used by Riak to correlate updates.
   */
  protected final String RIAK_CLIENT_ID = getClass().getName() + "/1.0";

  /**
   * For converting objects to/from other kinds of objects.
   */
  protected ConversionService conversionService = ConversionServiceFactory
      .createDefaultConversionService();
  /**
   * For caching objects based on ETags.
   */
  protected ConcurrentSkipListMap<BucketKeyPair, RiakValue<?>> cache = new ConcurrentSkipListMap<BucketKeyPair, RiakValue<?>>();
  /**
   * Whether or not to use the ETag-based cache.
   */
  protected boolean useCache = true;
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
  protected List<BucketKeyResolver> bucketKeyResolvers = new ArrayList<BucketKeyResolver>();
  /**
   * The default QosParameters to use for all operations through this template.
   */
  protected QosParameters defaultQosParameters = null;
  /**
   * {@link java.util.concurrent.ExecutorService} to use for running asynchronous jobs.
   */
  protected ExecutorService workerPool = Executors.newCachedThreadPool();
  /**
   * Default type to use when trying to deserialize objects and we can't otherwise tell what to
   * do.
   */
  protected Class<?> defaultType = String.class;
  /**
   * ClassLoader to use for saving/loading objects using the automatic converters.
   */
  protected ClassLoader classLoader = null;

  /**
   * Take all the defaults.
   */
  public AbstractRiakTemplate() {
    setRestTemplate(new RestTemplate());
  }

  /**
   * Use the specified {@link org.springframework.http.client.ClientHttpRequestFactory}.
   *
   * @param requestFactory
   */
  public AbstractRiakTemplate(ClientHttpRequestFactory requestFactory) {
    super(requestFactory);
    setRestTemplate(new RestTemplate());
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

  public boolean isUseCache() {
    return useCache;
  }

  public void setUseCache(boolean useCache) {
    this.useCache = useCache;
  }

  public QosParameters getDefaultQosParameters() {
    return defaultQosParameters;
  }

  public void setDefaultQosParameters(QosParameters defaultQosParameters) {
    this.defaultQosParameters = defaultQosParameters;
  }

  public ExecutorService getWorkerPool() {
    return workerPool;
  }

  public void setWorkerPool(ExecutorService workerPool) {
    this.workerPool = workerPool;
  }

  public void setIgnoreNotFound(boolean b) {
    if (b) {
      getRestTemplate().setErrorHandler(new Ignore404sErrorHandler());
    } else {
      if (getRestTemplate().getErrorHandler() instanceof Ignore404sErrorHandler) {
        getRestTemplate().setErrorHandler(new DefaultResponseErrorHandler());
      }
    }
  }

  public boolean getIgnoreNotFound() {
    return (getRestTemplate().getErrorHandler() instanceof Ignore404sErrorHandler);
  }

  /**
   * Get the default type to use if none can be inferred.
   *
   * @return
   */
  public Class<?> getDefaultType() {
    return defaultType;
  }

  /**
   * Set the default type to use if none can be inferred.
   *
   * @param defaultType
   */
  public void setDefaultType(Class<?> defaultType) {
    this.defaultType = defaultType;
  }

  public void setBeanClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  public String getHost() {
    Matcher m = prefix.matcher(defaultUri);
    if (m.matches()) {
      return m.group(1);
    }
    return "localhost";
  }

  public Integer getPort() {
    Matcher m = prefix.matcher(defaultUri);
    if (m.matches()) {
      return new Integer(m.group(2));
    }
    return 8098;
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

  public void afterPropertiesSet() throws Exception {
    Assert.notNull(conversionService,
        "Must specify a valid ConversionService.");

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

  @SuppressWarnings({"unchecked"})
  protected BucketKeyPair resolveBucketKeyPair(Object key, Object val) {
    BucketKeyResolver resolver = null;
    for (BucketKeyResolver r : bucketKeyResolvers) {
      if (r.canResolve(key)) {
        resolver = r;
        break;
      }
    }
    if (null == resolver) {
      resolver = new SimpleBucketKeyResolver();
    }

    BucketKeyPair bucketKeyPair = resolver.resolve(key);
    if (null == bucketKeyPair.getBucket() && null != val) {
      // No bucket specified, check for an annotation that specified bucket name.
      Annotation meta = (val instanceof Class ? (Class) val : val.getClass()).getAnnotation(
          org.springframework.data.keyvalue.riak.convert.KeyValueStoreMetaData.class);
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

  protected MediaType extractMediaType(Object value) {
    MediaType mediaType = (value instanceof byte[] ? MediaType.APPLICATION_OCTET_STREAM : MediaType.APPLICATION_JSON);
    if (null != value && value.getClass().getAnnotations().length > 0) {
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

  @SuppressWarnings({"unchecked"})
  protected <T> RiakValue<T> extractValue(final ResponseEntity<?> response, Class<?> origType,
                                          Class<T> requiredType) throws
      IOException {
    if (response.hasBody()) {
      RiakMetaData meta = extractMetaData(response.getHeaders());
      Object o = response.getBody();
      if (!origType.equals(requiredType)) {
        if (conversionService.canConvert(origType, requiredType)) {
          o = conversionService.convert(o, requiredType);
        } else {
          if (o instanceof byte[] || o instanceof String) {
            // Peek inside, see if it's a string of something we recognize
            String s = (o instanceof byte[] ? new String((byte[]) o) : (String) o);
            if (s.charAt(0) == '{' || s.charAt(0) == '[') {
              // Looks like it might be a JSON string. Use the JSON converter
              for (HttpMessageConverter conv : getRestTemplate().getMessageConverters()) {
                if (conv instanceof MappingJacksonHttpMessageConverter) {
                  o = conv.read(requiredType, new HttpInputMessage() {
                    public InputStream getBody() throws IOException {
                      Object body = response.getBody();
                      return new ByteArrayInputStream(
                          (body instanceof byte[] ? (byte[]) body : ((String) body)
                              .getBytes()));
                    }

                    public HttpHeaders getHeaders() {
                      return response.getHeaders();
                    }
                  });
                  break;
                }
              }

            }
          } else {
            throw new DataStoreOperationException(
                "Cannot convert object of type " + origType + " to type " + requiredType);
          }
        }
      }
      return new RiakValue<T>((T) o, meta);
    }
    return null;
  }

  @SuppressWarnings({"unchecked"})
  protected <K, T> T checkCache(K key, Class<T> requiredType) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, requiredType);
    RiakValue<?> obj = cache.get(bucketKeyPair);
    if (null != obj) {
      String bucketName = (null != bucketKeyPair.getBucket() ? bucketKeyPair.getBucket()
          .toString() : requiredType.getName());
      RestTemplate restTemplate = getRestTemplate();
      try {
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
      } catch (ResourceAccessException ignored) {
        return null;
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
    } else if (null != defaultQosParameters && null != defaultQosParameters
        .getReadThreshold()) {
      params.add(String.format("r=%s", defaultQosParameters.getReadThreshold()));
    }
    if (null != qosParams.getWriteThreshold()) {
      params.add(String.format("w=%s", qosParams.<Object>getWriteThreshold()));
    } else if (null != defaultQosParameters && null != defaultQosParameters
        .getWriteThreshold()) {
      params.add(String.format("w=%s", defaultQosParameters.getWriteThreshold()));
    }
    if (null != qosParams.getDurableWriteThreshold()) {
      params.add(String.format("dw=%s", qosParams.<Object>getDurableWriteThreshold()));
    } else if (null != defaultQosParameters && null != defaultQosParameters
        .getDurableWriteThreshold()) {
      params.add(String.format("dw=%s", defaultQosParameters.getDurableWriteThreshold()));
    }

    return (params.size() > 0 ? "?" + StringUtils.collectionToDelimitedString(
        params,
        "&") : "");
  }

  protected HttpHeaders defaultHeaders(Map<String, ?> metadata) {
    HttpHeaders headers = new HttpHeaders();
    headers.set("X-Riak-ClientId", RIAK_CLIENT_ID);
    if (null != metadata) {
      for (Map.Entry<String, ?> entry : metadata.entrySet()) {
        Object o = entry.getValue();
        headers.set(entry.getKey(), (null != o ? o.toString() : null));
      }
    }
    return headers;
  }

  protected <B, K> Class<?> getType(B bucket, K key) {
    return getType(bucket, key, getClass().getClassLoader());
  }

  protected <B, K> Class<?> getType(B bucket, K key, ClassLoader classLoader) {
    Class<?> clazz = null;
    try {
      HttpHeaders headers = getRestTemplate().headForHeaders(defaultUri, bucket, key);
      if (null != headers) {
        String s = headers.getFirst(RIAK_META_CLASSNAME);
        if (null != s) {
          try {
            if (null != classLoader) {
              clazz = Class.forName(s, false, classLoader);
            } else {
              clazz = Class.forName(s);
            }
          } catch (ClassNotFoundException ignored) {
          }
        }
      }
      if (null == clazz) {
        if (headers.getContentType().equals(MediaType.APPLICATION_JSON)) {
          clazz = Map.class;
        } else if (headers.getContentType().equals(MediaType.TEXT_PLAIN)) {
          clazz = String.class;
        } else {
          // handle as bytes
          log.error("Need to handle bytes!");
          clazz = byte[].class;
        }
      }
    } catch (ResourceAccessException notFound) {
      clazz = String.class;
    }
    return clazz;
  }

}
