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

package org.springframework.data.keyvalue.riak.groovy;

import groovy.lang.Closure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.keyvalue.riak.DataStoreOperationException;
import org.springframework.data.keyvalue.riak.core.AsyncKeyValueStoreOperation;
import org.springframework.data.keyvalue.riak.core.AsyncRiakTemplate;
import org.springframework.data.keyvalue.riak.core.KeyValueStoreMetaData;
import org.springframework.data.keyvalue.riak.core.QosParameters;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakOperation<T> implements Callable {

  static enum Type {
    SET, SETASBYTES, PUT, GET, GETASBYTES, GETASTYPE, CONTAINSKEY, DELETE, FOREACH
  }

  static String COMPLETED = "completed";
  static String FAILED = "failed";

  protected final Logger log = LoggerFactory.getLogger(getClass());

  protected AsyncRiakTemplate riak;
  protected Type type;
  protected String bucket;
  protected String key;
  protected T value;
  protected Class<?> requiredType = null;
  protected long timeout = -1L;
  protected QosParameters qosParameters;
  protected Map<String, List<GuardedClosure>> callbacks = new LinkedHashMap<String, List<GuardedClosure>>();
  protected ClosureInvokingCallback callbackInvoker = new ClosureInvokingCallback();

  public RiakOperation(AsyncRiakTemplate riak, Type type) {
    this.riak = riak;
    this.type = type;
  }

  public Type getType() {
    return type;
  }

  public Map<String, List<GuardedClosure>> getCallbacks() {
    return callbacks;
  }

  public QosParameters getQosParameters() {
    return qosParameters;
  }

  public void setQosParameters(QosParameters qosParameters) {
    this.qosParameters = qosParameters;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }

  public Class<?> getRequiredType() {
    return requiredType;
  }

  public void setRequiredType(Class<?> requiredType) {
    this.requiredType = requiredType;
  }

  public long getTimeout() {
    return timeout;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  public void addHandler(String type, Closure handler, Closure guard) {
    List<GuardedClosure> guardedClosures = callbacks.get(type);
    if (null == guardedClosures) {
      guardedClosures = new ArrayList<GuardedClosure>();
      callbacks.put(type, guardedClosures);
    }
    guardedClosures.add(new GuardedClosure(handler, guard));
  }

  @SuppressWarnings({"unchecked"})
  public Object call() throws Exception {
    Future<?> f = null;
    switch (type) {
      case GET:
        f = riak.get(bucket, key, callbackInvoker);
        break;
      case GETASBYTES:
        f = riak.getAsBytes(bucket, key, callbackInvoker);
        break;
      case GETASTYPE:
        f = riak.getAsType(bucket, key, requiredType, callbackInvoker);
        break;
      case PUT:
        f = riak.put(bucket, value, callbackInvoker);
        break;
      case SET:
        f = riak.set(bucket, key, value, callbackInvoker);
        break;
      case SETASBYTES:
        byte[] bytes;
        if (value instanceof byte[]) {
          bytes = (byte[]) value;
        } else {
          bytes = riak.getConversionService().convert(value, byte[].class);
        }
        f = riak.setAsBytes(bucket, key, bytes, callbackInvoker);
        break;
      case CONTAINSKEY:
        f = riak.containsKey(bucket, key, callbackInvoker);
        break;
      case DELETE:
        f = riak.delete(bucket, key, callbackInvoker);
        break;
      case FOREACH:
        f = riak.getBucketSchema(bucket,
            null,
            new AsyncKeyValueStoreOperation<Map<String, Object>, Object>() {
              public Object completed(KeyValueStoreMetaData meta, Map<String, Object> result) {
                List<Object> results = new LinkedList<Object>();
                List<String> keys = (List<String>) result.get("keys");
                for (String key : keys) {
                  try {
                    Future<?> getFut = riak.get(bucket, key, callbackInvoker);
                    if (timeout > 0) {
                      Object o = getFut.get(timeout, TimeUnit.MILLISECONDS);
                      if (null != o) {
                        results.add(o);
                      }
                    } else if (timeout < 0) {
                      Object o = getFut.get();
                      if (null != o) {
                        results.add(o);
                      }
                    } else {
                      results.add(getFut);
                    }
                  } catch (InterruptedException e) {
                    throw new DataStoreOperationException(e.getMessage(), e);
                  } catch (ExecutionException e) {
                    throw new DataStoreOperationException(e.getMessage(), e);
                  } catch (TimeoutException e) {
                    throw new DataStoreOperationException(e.getMessage(), e);
                  }
                }
                return (results.size() > 0 ? results : null);
              }

              public Object failed(Throwable error) {
                throw new RuntimeException(error);
              }
            });
        break;
    }

    if (null != f) {
      if (timeout > 0) {
        // Block until finished or timeout
        return f.get(timeout, TimeUnit.MILLISECONDS);
      } else if (timeout < 0) {
        // Block indefinitely
        return f.get();
      }
    }

    return f;
  }

  class GuardedClosure {

    private Closure delegate;
    private Closure guard;

    GuardedClosure(Closure delegate, Closure guard) {
      this.delegate = delegate;
      this.guard = guard;
    }

    public Closure getDelegate() {
      return delegate;
    }

    public Closure getGuard() {
      return guard;
    }

  }

  class ClosureInvokingCallback implements AsyncKeyValueStoreOperation {

    public Object completed(KeyValueStoreMetaData meta, Object result) {
      if (!callbacks.containsKey(COMPLETED)) {
        return new Object[]{result, meta};
      }
      for (GuardedClosure cl : callbacks.get(COMPLETED)) {
        boolean execute = true;

        Closure guardExpr = cl.getGuard();
        if (null != guardExpr) {
          int noOfParams = guardExpr.getParameterTypes().length;
          Object guardResult;
          if (noOfParams == 2) {
            guardResult = guardExpr.call(new Object[]{result, meta});
          } else {
            guardResult = guardExpr.call(result);
          }
          if (null != guardResult) {
            if (guardResult instanceof Boolean) {
              execute = (Boolean) guardResult;
            } else {
              execute = true;
            }
          }
        }

        if (execute) {
          Closure callback = cl.getDelegate();
          if (callback.getParameterTypes().length == 2) {
            return callback.call(new Object[]{result, meta});
          } else {
            return callback.call(result);
          }
        }
      }
      return null;
    }

    public Object failed(Throwable error) {
      if (!callbacks.containsKey(FAILED)) {
        throw new RuntimeException(error);
      }
      for (GuardedClosure cl : callbacks.get(FAILED)) {
        boolean execute = true;
        Closure guardExpr = cl.getGuard();
        if (null != guardExpr) {
          Object guardResult = guardExpr.call(error);
          if (null != guardResult) {
            if (guardResult instanceof Boolean) {
              execute = (Boolean) guardResult;
            } else {
              execute = true;
            }
          }
        }

        if (execute) {
          Closure callback = cl.getDelegate();
          return callback.call(error);
        }
      }
      return null;
    }

  }

}
