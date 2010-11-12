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

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.util.concurrent.Callable;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public abstract class AbstractAsyncOperation<T> implements Callable<T>, InitializingBean {

  protected RiakTemplate riakTemplate;

  protected AbstractAsyncOperation() {
  }

  protected AbstractAsyncOperation(RiakTemplate riakTemplate) {
    this.riakTemplate = riakTemplate;
  }

  public RiakTemplate getRiakTemplate() {
    return riakTemplate;
  }

  public void setRiakTemplate(RiakTemplate riakTemplate) {
    this.riakTemplate = riakTemplate;
  }

  public void afterPropertiesSet() throws Exception {
    Assert.notNull(riakTemplate, "Must provide a configured RiakTemplate for this operation.");
  }

}
