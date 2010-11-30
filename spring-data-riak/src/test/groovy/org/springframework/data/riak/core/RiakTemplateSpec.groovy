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
package org.springframework.data.riak.core

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.data.riak.mapreduce.JavascriptMapReduceOperation
import org.springframework.data.riak.mapreduce.MapReduceJob
import org.springframework.data.riak.mapreduce.RiakMapReducePhase
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@ContextConfiguration(locations = "/org/springframework/data/RiakTemplateTests.xml")
class RiakTemplateSpec extends Specification {

  @Autowired
  ApplicationContext appCtx
  @Autowired
  RiakTemplate riak
  int run = 1

  def "Test Map object"() {

    given:
    def val = "value"
    def objIn = [test: val, integer: 12]
    riak.set("test:test", objIn)

    when:
    def objOut = riak.get("test:test")

    then:
    objOut.test == val

  }

  def "Test custom object"() {

    given:
    TestObject objIn = new TestObject()
    riak.set("${TestObject.name}:test", objIn)

    when:
    TestObject objOut = riak.get("${TestObject.name}:test")

    then:
    objOut.test == "value"

  }

  def "Test getting bucket schema"() {

    when:
    def schema = riak.getBucketSchema("test", true)

    then:
    "test" == schema.props.name

  }

  def "Test updating bucket schema"() {

    when:
    def schema = riak.updateBucketSchema("test", [n_val: 2]).getBucketSchema("test")

    then:
    2 == schema.props.n_val

  }

  def "Test get with metadata"() {

    when:
    def val = riak.getWithMetaData([bucket: "test", key: "test"], LinkedHashMap)

    then:
    val.metaData.properties["Server"].contains("WebMachine")

  }

  def "Test containsKey"() {

    when:
    def containsKey = riak.containsKey([bucket: "test", key: "test"])

    then:
    true == containsKey

  }

  def "Test linking"() {

    given:
    riak.link("${TestObject.name}:test", "test:test", "test")

    when:
    def val = riak.getWithMetaData("test:test", Map)
    def result = val.metaData.properties["Link"].collect { it.contains("riaktag=\"test\"") }

    then:
    1 == result.size()

  }

  def "Test link walking"() {

    when:
    def val = riak.linkWalk("test:test", "test")

    then:
    null != val
    1 == val.size()
    val.get(0) instanceof TestObject

  }

  def "Test multiple get"() {

    when:
    def objs = riak.getValues([
        new SimpleBucketKeyPair("test", "test"),
        new SimpleBucketKeyPair(TestObject.name, "test")
    ])

    then:
    2 == objs.size()

  }

  def "Test getAndSet with Map"() {

    given:
    def i = run++
    def newObj = [test: "value $i", integer: 12]

    when:
    def oldObj = riak.getAndSet("test:test", newObj)

    then:
    "value" == oldObj.test

  }

  def "Test setMultipleIfKeysNonExistent with Map"() {

    given:
    def testKey = new SimpleBucketKeyPair("test", "test")
    def testKey2 = new SimpleBucketKeyPair(TestObject.name, "test")
    def newObj = [:]
    newObj[testKey] = [test: "value", integer: 12]
    newObj[testKey2] = [test: "value", integer: 12]

    when:
    def secondObj = riak.setMultipleIfKeysNonExistent(newObj).get(testKey2)
    secondObj.test = "newValue"
    def updObj = [:]
    updObj[testKey2] = secondObj
    def thirdObj = riak.setMultipleIfKeysNonExistent(updObj).get(testKey2)

    then:
    "value" == thirdObj.test

  }

  def "Test Map/Reduce returning Integer"() {

    given:
    MapReduceJob job = riak.createMapReduceJob()
    def mapJs = new JavascriptMapReduceOperation("function(v){ var o=Riak.mapValuesJson(v); return [1]; }")
    def mapPhase = new RiakMapReducePhase("map", "javascript", mapJs)

    def reduceJs = new JavascriptMapReduceOperation("Riak.reduceSum")
    def reducePhase = new RiakMapReducePhase("reduce", "javascript", reduceJs)

    job.addInputs(["test"]).
        addPhase(mapPhase).
        addPhase(reducePhase)
    println job.toJson()

    when:
    def result = riak.execute(job, Integer)

    then:
    1 == result

  }

  def "Test Map/Reduce returning List"() {

    given:
    MapReduceJob job = riak.createMapReduceJob()
    def mapJs = new JavascriptMapReduceOperation("function(v){ var o=Riak.mapValuesJson(v); return [1]; }")
    def mapPhase = new RiakMapReducePhase("map", "javascript", mapJs)

    def reduceJs = new JavascriptMapReduceOperation("Riak.reduceSum")
    def reducePhase = new RiakMapReducePhase("reduce", "javascript", reduceJs)

    job.addInputs(["test"]).
        addPhase(mapPhase).
        addPhase(reducePhase)

    when:
    def result = riak.execute(job)

    then:
    1 == result.size()
    1 == result[0]

  }

  def "Test deleteKeys"() {

    given:
    def testKey = new SimpleBucketKeyPair("test", "test")
    def testKey2 = new SimpleBucketKeyPair(TestObject.name, "test")

    when:
    def deleted = riak.deleteKeys(testKey, testKey2)

    then:
    true == deleted

  }

}