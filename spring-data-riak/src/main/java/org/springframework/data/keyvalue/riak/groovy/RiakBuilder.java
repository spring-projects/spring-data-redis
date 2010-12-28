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
import groovy.util.BuilderSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.keyvalue.riak.DataStoreOperationException;
import org.springframework.data.keyvalue.riak.core.AsyncRiakTemplate;
import org.springframework.data.keyvalue.riak.core.RiakQosParameters;
import org.springframework.data.keyvalue.riak.core.SimpleBucketKeyPair;
import org.springframework.data.keyvalue.riak.mapreduce.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Groovy Builder that implements a powerful and syntactically succinct DSL for Riak datastore
 * access using SDKV for Riak's {@link AsyncRiakTemplate}.
 * <p/>
 * The DSL responds to most of the important methods from the <code>AsyncRiakTemplate</code>:
 * <ul><li>set</li><li>setAsBytes</li><li>put</li><li>get</li><li>getAsBytes</li>
 * <li>getAsType</li><li>containsKey</li><li>delete</li><li>foreach</li></ul>
 * <p/>
 * An example of DSL usage (to delete all entries in a bucket):
 * <pre><code>riak.foreach(bucket: "test") {
 *   completed { v, meta ->
 *     delete(bucket: "test", key: meta.key)
 *   }
 * }
 * </code></pre>
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakBuilder extends BuilderSupport {

  protected final Logger log = LoggerFactory.getLogger(getClass());
  @Autowired(required = false)
  protected AsyncRiakTemplate riak;
  @Autowired(required = false)
  protected ExecutorService workerPool = Executors.newCachedThreadPool();
  protected String defaultBucketName;
  protected List<Object> results = new LinkedList<Object>();

  public RiakBuilder() {
  }

  public RiakBuilder(AsyncRiakTemplate riak) {
    this.riak = riak;
  }

  public RiakBuilder(AsyncRiakTemplate riak, ExecutorService workerPool) {
    this.riak = riak;
    this.workerPool = workerPool;
  }

  public RiakBuilder(BuilderSupport proxyBuilder, AsyncRiakTemplate riak) {
    super(proxyBuilder);
    this.riak = riak;
  }

  public RiakBuilder(Closure nameMappingClosure, BuilderSupport proxyBuilder, AsyncRiakTemplate riak) {
    super(nameMappingClosure, proxyBuilder);
    this.riak = riak;
  }

  public AsyncRiakTemplate getAsyncTemplate() {
    return riak;
  }

  public void setAsyncTemplate(AsyncRiakTemplate riak) {
    this.riak = riak;
  }

  public ExecutorService getWorkerPool() {
    return workerPool;
  }

  public void setWorkerPool(ExecutorService workerPool) {
    this.workerPool = workerPool;
  }

  @Override
  protected void setParent(Object parent, Object child) {
    log.debug("setParent/2 " + parent + " " + child);
  }

  @SuppressWarnings({"unchecked"})
  @Override
  protected Object createNode(Object name) {
    log.debug("createNode/1 " + name);
    if ("call".equals(name)) {
      // IGNORED
    } else if ("foreach".equals(name)) {
      RiakOperation<Object> op = new RiakOperation<Object>(riak, RiakOperation.Type.FOREACH);
      op.setBucket(defaultBucketName);
      return op;
    } else if ("mapreduce".equals(name)) {
      return createMapReduceJob();
    } else if ("query".equals(name)) {
      QueryPhase p = new QueryPhase();
      p.job = ((RiakMapReduceOperation) getCurrent()).getJob();
      return getCurrent();
    } else if ("map".equals(name) || "reduce".equals(name)) {
      QueryPhase p = new QueryPhase();
      p.job = ((RiakMapReduceOperation) getCurrent()).getJob();
      p.phase = name.toString();
      return p;
    } else {
      defaultBucketName = name.toString();
    }

    return null;
  }

  @SuppressWarnings({"unchecked"})
  @Override
  protected Object createNode(Object name, Object value) {
    log.debug("createNode/2 " + name + " " + value);
    if ("inputs".equals(name)) {
      AsyncRiakMapReduceJob job = ((RiakMapReduceOperation) getCurrent()).getJob();
      if (null != value && value instanceof String) {
        List<String> keys = new ArrayList<String>();
        keys.add(value.toString());
        job.addInputs(keys);
      } else if (value instanceof List) {
        job.addInputs((List) value);
      }
      return job;
    } else if ("language".equals(name)) {
      QueryPhase p = (QueryPhase) getCurrent();
      p.language = value.toString();
      return p;
    } else if ("source".equals(name)) {
      QueryPhase p = (QueryPhase) getCurrent();
      p.source = value.toString();
      return p;
    } else if ("keep".equals(name)) {
      QueryPhase p = (QueryPhase) getCurrent();
      p.keep = (value instanceof Boolean ? (Boolean) value : new Boolean(value.toString()));
      return p;
    } else if ("arg".equals(name)) {
      QueryPhase p = (QueryPhase) getCurrent();
      p.arg = value;
      return p;
    }
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @SuppressWarnings({"unchecked"})
  @Override
  protected Object createNode(Object name, Map attributes) {
    log.debug("createNode/2 (Map) " + name + " " + attributes);

    if ("mapreduce".equals(name)) {
      RiakMapReduceOperation oper = createMapReduceJob();
      // Set timeout
      Object o = attributes.get("wait");
      if (null != o) {
        if (o instanceof Long) {
          oper.setTimeout((Long) o);
        } else if (o instanceof String) {
          oper.setTimeout(new Long(o.toString()));
        } else if (o instanceof Integer) {
          oper.setTimeout(new Long((Integer) o));
        } else {
          throw new IllegalArgumentException(
              "Timeout should be an Integer, a Long, or a String denoting milliseconds");
        }
      }
      return oper;
    } else if ("map".equals(name) || "reduce".equals(name)) {
      QueryPhase p = new QueryPhase();
      p.job = ((RiakMapReduceOperation) getCurrent()).getJob();
      p.phase = name.toString();
      // Set arg
      p.arg = attributes.get("arg");
      return p;
    } else {
      RiakOperation.Type type = RiakOperation.Type.valueOf(name.toString().toUpperCase());
      if (null != type) {
        RiakOperation op = new RiakOperation<Object>(riak, type);
        // Set a bucket name
        Object o = attributes.get("bucket");
        if (null == o && null != defaultBucketName) {
          op.setBucket(defaultBucketName);
        } else {
          op.setBucket((null != o ? o.toString() : null));
        }
        // Set the object's key
        o = attributes.get("key");
        op.setKey((null != o ? o.toString() : null));
        // Set the value
        o = attributes.get("value");
        op.setValue(o);
        // Set the type of object (for getAsType)
        o = attributes.get("type");
        if (null != o) {
          if (o instanceof Class) {
            op.setRequiredType((Class<?>) o);
          } else if (o instanceof String) {
            try {
              op.setRequiredType(Class.forName((String) o));
            } catch (ClassNotFoundException e) {
              throw new DataStoreOperationException(e.getMessage(), e);
            }
          } else {
            op.setRequiredType(o.getClass());
          }
        }
        // Set QOS parameters
        o = attributes.get("qos");
        if (null != o) {
          RiakQosParameters qos = new RiakQosParameters();
          Map<String, Object> qosParams = (Map<String, Object>) o;
          if (qosParams.containsKey("dw")) {
            qos.setDurableWriteThreshold(qosParams.get("dw"));
          }
          if (qosParams.containsKey("w")) {
            qos.setWriteThreshold(qosParams.get("w"));
          }
          if (qosParams.containsKey("r")) {
            qos.setReadThreshold(qosParams.get("r"));
          }
          op.setQosParameters(qos);
        }
        // Set timeout
        o = attributes.get("wait");
        if (null != o) {
          if (o instanceof Long) {
            op.setTimeout((Long) o);
          } else if (o instanceof String) {
            op.setTimeout(new Long(o.toString()));
          } else if (o instanceof Integer) {
            op.setTimeout(new Long((Integer) o));
          } else {
            throw new IllegalArgumentException(
                "Timeout should be an Integer, a Long, or a String denoting milliseconds");
          }
        }

        return op;
      }
    }
    return null;
  }

  @Override
  protected Object createNode(Object name, Map attributes, Object value) {
    log.debug("createNode/3");
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Object invokeMethod(String methodName) {
    log.debug("invokeMethod/1 " + methodName);
    return super.invokeMethod(methodName);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public Object invokeMethod(String methodName, Object arg) {
    if (log.isDebugEnabled()) {
      log.debug("invokeMethod/2: " + methodName + " " + arg);
    }
    if ("completed".equals(methodName) || "failed".equals(methodName)) {
      if (getCurrent() instanceof RiakOperation) {
        RiakOperation<Object> op = (RiakOperation<Object>) getCurrent();
        Object[] args = (Object[]) arg;
        Map<String, Object> params;
        Closure handler = null;
        Closure guard = null;
        for (Object o : args) {
          if (o instanceof Map) {
            params = (Map<String, Object>) o;
            if (params.containsKey("when")) {
              guard = (Closure) params.get("when");
            }
          } else if (o instanceof Closure) {
            handler = (Closure) o;
          }
        }
        op.addHandler(methodName, handler, guard);
        return op;
      } else if (getCurrent() instanceof RiakMapReduceOperation) {
        RiakMapReduceOperation oper = (RiakMapReduceOperation) getCurrent();
        Object[] args = (Object[]) arg;
        if ("completed".equals(methodName)) {
          oper.setCompleted((Closure) args[0]);
        } else if ("failed".equals(methodName)) {
          oper.setFailed((Closure) args[0]);
        }
        return oper;
      }
    } else if ("call".equals(methodName)) {
      results.clear();
      defaultBucketName = null;
    }
    // By default
    return super.invokeMethod(methodName, arg);
  }

  @SuppressWarnings({"unchecked"})
  @Override
  protected void nodeCompleted(Object parent, Object node) {
    if (log.isDebugEnabled()) {
      log.debug("nodeCompleted: parent=" + parent + ", node=" + node);
    }
    if (parent instanceof RiakMapReduceOperation && node instanceof QueryPhase) {
      QueryPhase p = (QueryPhase) node;
      MapReduceOperation oper = null;
      if ("javascript".equals(p.language)) {
        if (null != p.source) {
          oper = new JavascriptMapReduceOperation(p.source);
        } else if (null != p.bucket && null != p.key) {
          oper = new JavascriptMapReduceOperation(new SimpleBucketKeyPair(p.bucket, p.key));
        }
      } else {
        oper = new ErlangMapReduceOperation(p.module, p.func);
      }
      if (null != oper) {
        RiakMapReducePhase phase = new RiakMapReducePhase(p.phase, p.language, oper);
        if (null != p.keep) {
          phase.setKeepResults(p.keep);
        }
        phase.setArg(p.arg);
        p.job.addPhase(phase);
      }
    } else {
      super.nodeCompleted(parent, node);
    }
  }

  @SuppressWarnings({"unchecked"})
  @Override
  protected Object postNodeCompletion(Object parent, Object node) {
    if (log.isDebugEnabled()) {
      log.debug("postNodeCompletion: " + parent + " " + node);
    }
    if (node instanceof RiakOperation) {
      RiakOperation<Object> op = (RiakOperation<Object>) node;
      try {
        Object o = op.call();
        if (null != o) {
          results.add(o);
        }
        return o;
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    } else if (null == parent && node instanceof RiakMapReduceOperation) {
      RiakMapReduceOperation oper = (RiakMapReduceOperation) node;
      try {
        Object o = oper.call();
        if (null != o) {
          results.add(o);
        }
        return o;
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    }

    return super.postNodeCompletion(parent, node);
  }

  protected RiakMapReduceOperation createMapReduceJob() {
    AsyncRiakMapReduceJob job = new AsyncRiakMapReduceJob(riak);
    if (null != defaultBucketName) {
      List<String> keys = new ArrayList<String>();
      keys.add(defaultBucketName);
      job.addInputs(keys);
    }
    return new RiakMapReduceOperation(riak, job);
  }

  private class QueryPhase {

    AsyncRiakMapReduceJob job;
    String phase;
    String language = "javascript";
    String source = null;
    String bucket = null;
    String key = null;
    String module = null;
    String func = null;
    Boolean keep = null;
    Object arg = null;

  }

}
