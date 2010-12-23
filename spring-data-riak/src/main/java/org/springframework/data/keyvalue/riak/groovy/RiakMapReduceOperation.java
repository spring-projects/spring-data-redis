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
import org.springframework.data.keyvalue.riak.core.AsyncKeyValueStoreOperation;
import org.springframework.data.keyvalue.riak.core.AsyncRiakTemplate;
import org.springframework.data.keyvalue.riak.core.KeyValueStoreMetaData;
import org.springframework.data.keyvalue.riak.mapreduce.AsyncRiakMapReduceJob;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakMapReduceOperation implements Callable {

  protected final Logger log = LoggerFactory.getLogger(getClass());

  protected AsyncRiakTemplate riak;
  protected AsyncRiakMapReduceJob job;
  protected Long timeout = -1L;
  protected Closure completed = null;
  protected Closure failed = null;

  public RiakMapReduceOperation(AsyncRiakTemplate riak, AsyncRiakMapReduceJob job) {
    this.riak = riak;
    this.job = job;
  }

  public AsyncRiakMapReduceJob getJob() {
    return job;
  }

  public void setJob(AsyncRiakMapReduceJob job) {
    this.job = job;
  }

  public Long getTimeout() {
    return timeout;
  }

  public void setTimeout(Long timeout) {
    this.timeout = timeout;
  }

  public Closure getCompleted() {
    return completed;
  }

  public void setCompleted(Closure completed) {
    this.completed = completed;
  }

  public Closure getFailed() {
    return failed;
  }

  public void setFailed(Closure failed) {
    this.failed = failed;
  }

  public Object call() throws Exception {
    Future<?> f = riak.execute(job, new AsyncKeyValueStoreOperation<List<?>, Object>() {
      public Object completed(KeyValueStoreMetaData meta, List<?> result) {
        Object arg = new Object[]{result, meta};
        if (null != completed) {
          return completed.call(arg);
        } else {
          return new Object[]{result, meta};
        }
      }

      public Object failed(Throwable error) {
        if (null != failed) {
          return failed.call(error);
        } else {
          throw new RuntimeException(error);
        }
      }
    });

    if (timeout == 0) {
      return f;
    } else if (timeout < 0) {
      return f.get();
    } else {
      return f.get(timeout, TimeUnit.MILLISECONDS);
    }
  }
}
