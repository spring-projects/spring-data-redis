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
import org.springframework.data.keyvalue.riak.core.AsyncRiakTemplate;
import org.springframework.data.keyvalue.riak.core.RiakQosParameters;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakBuilder extends BuilderSupport {

  protected final Logger log = LoggerFactory.getLogger(getClass());
  @Autowired
  protected AsyncRiakTemplate riak;
  @Autowired
  protected ExecutorService workerPool = Executors.newCachedThreadPool();

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

  @Override
  protected Object createNode(Object name) {
    log.debug("createNode/1 " + name);
    return this;
  }

  @Override
  protected Object createNode(Object name, Object value) {
    log.debug("createNode/2 " + name + " " + value);
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @SuppressWarnings({"unchecked"})
  @Override
  protected Object createNode(Object name, Map attributes) {
    log.debug("createNode/2 (Map) " + name + " " + attributes);
    RiakOperation.Type type = RiakOperation.Type.valueOf(name.toString().toUpperCase());
    if (null != type) {
      RiakOperation op = new RiakOperation<Object>(riak, type);
      Object o = attributes.get("bucket");
      op.setBucket((null != o ? o.toString() : null));
      o = attributes.get("key");
      op.setKey((null != o ? o.toString() : null));
      o = attributes.get("value");
      op.setValue(o);
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
      log.debug("invokeMethod: " + methodName + " " + arg);
    }
    if ("completed".equals(methodName) || "failed".equals(methodName)) {
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
    }
    return super.invokeMethod(methodName, arg);
  }

  @SuppressWarnings({"unchecked"})
  @Override
  protected void nodeCompleted(Object parent, Object node) {
    log.debug("nodeCompleted: " + parent + " " + node);
    if (null == parent && node instanceof RiakOperation) {
      RiakOperation<Object> op = (RiakOperation<Object>) node;
      try {
        op.call();
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    } else {
      super.nodeCompleted(parent, node);
    }
  }

  @Override
  protected Object postNodeCompletion(Object parent, Object node) {
    log.debug("postNodeCompletion: " + parent + " " + node);
    return super.postNodeCompletion(parent, node);
  }
}
