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
package org.springframework.datastore.riak.core

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@ContextConfiguration(locations = "/org/springframework/datastore/RiakTemplateTests.xml")
class RiakTemplateSpec extends Specification {

  @Autowired
  ApplicationContext appCtx
  @Autowired
  RiakTemplate riak
  int run = 1

  def "Test Map object with 'bucket:key' key"() {

    given:
    def i = run++
    String val = "value $i"
    def objIn = [test: "value $i", integer: 12]
    riak.set("test:test", objIn)

    when:
    def objOut = riak.get("test:test")

    then:
    objOut.test == val

  }

  def "Test Map object with Map key"() {

    given:
    def i = run++
    String val = "value $i"
    def objIn = [test: val, integer: 12]
    riak.set([bucket: "test", key: "test"], objIn)

    when:
    def objOut = riak.get([bucket: "test", key: "test"])

    then:
    objOut.test == val

  }

  def "Test custom object with 'bucket:key' key"() {

    given:
    TestObject objIn = new TestObject()
    riak.set("test:test", objIn)

    when:
    TestObject objOut = riak.get("test:test")

    then:
    objOut.test == "value"

  }

  def "Test custom object with 'ClassName:key' key"() {

    given:
    TestObject objIn = new TestObject()
    riak.set("test", objIn)

    when:
    TestObject objOut = riak.getAsType("test", TestObject)

    then:
    objOut.test == "value"

  }

  def "Test containsKey"() {

    when:
    def containsKey = riak.containsKey("test:test")

    then:
    true == containsKey

  }

  def "Test multiple get"() {

    when:
    def objs = riak.getValues(["test:test", "${TestObject.name}:test"])

    then:
    2 == objs.size()

  }

  def "Test getAndSet with Map"() {

    given:
    def i = run++
    String val = "value $i"
    def newObj = [test: val, integer: 12]

    when:
    def oldObj = riak.getAndSet("test:test", newObj)

    then:
    "value" == oldObj.test

  }

  def "Test deleteKeys"() {

    when:
    def deleted = riak.deleteKeys("test:test", "${TestObject.name}:test")

    then:
    true == deleted

  }

  def "Test setMultipleIfKeysNonExistent with Map"() {

    given:
    def newObj = [
        "test:test": [test: "value", integer: 12],
        "${TestObject.name}:test": [test: "value", integer: 12]
    ]

    when:
    def secondObj = riak.setMultipleIfKeysNonExistent(newObj).get("${TestObject.name}:test")
    secondObj.test = "newValue"
    def thirdObj = riak.setMultipleIfKeysNonExistent(["${TestObject.name}:test": secondObj]).get("${TestObject.name}:test")

    then:
    "value" == thirdObj.test

    cleanup:
    riak.deleteKeys("test:test", "${TestObject.name}:test")

  }

}
