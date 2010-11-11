/*
 * Copyright 2010 the original author or authors.
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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.LinkedHashMap;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/org/springframework/datastore/RiakTemplateTests.xml"})
@SuppressWarnings({"unchecked"})
public class RiakTemplateIntegrationTests {

  @Autowired
  ApplicationContext appCtx;
  @Autowired
  RiakTemplate riak;

  public void testSet() {
    Map obj = new LinkedHashMap();
    obj.put("test", "value");
    obj.put("test2", 12);
    riak.set("test:test", obj);
  }

  @Test
  public void testSetAsType() {
    TestObject obj = new TestObject();
    riak.set("test", obj);
  }

  public void testSetInferringType() {
    Map obj = new LinkedHashMap();
    obj.put("test", "value");
    obj.put("test2", 12);
    riak.set("test", obj);
  }

  public void testGetInferringType() {
    Map obj = riak.get("java.util.LinkedHashMap:test");
    assert null != obj;
    assert 12 == (Integer) obj.get("test2");
  }

  @Test
  public void testGetAsType() {
    TestObject obj = riak.getAsType("test", TestObject.class);
    assert null != obj;
  }

  @Test
  public void conversions() {

  }

}
