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
package org.springframework.data.keyvalue.riak.core

import org.springframework.data.keyvalue.riak.core.io.RiakFile
import org.springframework.data.keyvalue.riak.mapreduce.JavascriptMapReduceOperation
import org.springframework.data.keyvalue.riak.mapreduce.MapReduceJob
import org.springframework.data.keyvalue.riak.mapreduce.RiakMapReduceJob
import org.springframework.data.keyvalue.riak.mapreduce.RiakMapReducePhase
import spock.lang.Shared
import spock.lang.Specification

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
class RiakTemplateSpec extends Specification {

  @Shared RiakTemplate riak = new RiakTemplate()
  int run = 1
  @Shared def riakBin = System.properties["bamboo.RIAK_BIN"] ?: "/usr/sbin/riak"
  @Shared def p
  @Shared def id
  @Shared boolean shutdown = false

  def setupSpec() {
    RiakQosParameters qos = new RiakQosParameters()
    qos.setDurableWriteThreshold("all")
    riak.setDefaultQosParameters(qos)
    riak.ignoreNotFound = true

    if (!riak.get("status", "")) {
      p = "$riakBin start".execute()
      p.waitFor()
      shutdown = true
      Thread.sleep(2000)
    }

    riak.getBucketSchema("test", true).keys.each {
      riak.delete("test", it)
    }
    riak.getBucketSchema(TestObject.name, true).keys.each {
      riak.delete("test", it)
    }
  }

  def cleanupSpec() {
    if (shutdown) {
      p = "$riakBin stop".execute()
      p.waitFor()
    }
  }

  def "Test Map object"() {

    given:
    def val = "value"
    def objIn = [test: val, integer: 12]
    riak.set("test", "test", objIn)

    when:
    def objOut = riak.get("test", "test")

    then:
    objOut.test == val

  }

  def "Test generating ID for object"() {

    given:
    def val = "value"
    def objIn = [test: val, integer: 12]

    when:
    id = riak.put("test", objIn, null)

    then:
    null != id

  }

  def "Test custom object"() {

    given:
    TestObject objIn = new TestObject()
    riak.set(TestObject.name, "test", objIn)

    when:
    TestObject objOut = riak.getAsType(TestObject.name, "test", TestObject)

    then:
    objOut.test == "value"

  }

  def "Test convert custom object from bytes"() {

    given:
    def qos = new RiakQosParameters()
    qos.durableWriteThreshold = "all"
    riak.setAsBytes(TestObject.name, "test", "{\"test\":\"string data\",\"integer\":1}".bytes, qos)

    when:
    def objOut = riak.getAsType(TestObject.name, "test", TestObject)
    //riak.delete(TestObject.name, "test")

    then:
    objOut instanceof TestObject
    objOut.test == "string data"

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
    def val = riak.getWithMetaData("test", "test", LinkedHashMap)

    then:
    val.metaData.properties["Server"].contains("WebMachine")

  }

  def "Test setting QosParameters"() {

    given:
    def obj = riak.get("test", "test")

    when:
    def qos = new RiakQosParameters()
    qos.durableWriteThreshold = "all"
    riak.set("test", "test", obj, qos)

    then:
    true

  }

  def "Test containsKey"() {

    when:
    def containsKey = riak.containsKey("test", "test")

    then:
    true == containsKey

  }

  def "Test linking"() {

    given:
    def qos = new RiakQosParameters()
    qos.durableWriteThreshold = "all"
    riak.set(TestObject.name, "test", new TestObject(), qos)
    riak.link(TestObject.name, "test", "test", "test", "test")

    when:
    def val = riak.getWithMetaData("test", "test", Map)
    def result = val.metaData.properties["Link"].find { it.contains("riaktag=\"test\"") }

    then:
    null != result

  }

  def "Test link walking"() {

    when:
    def val = riak.linkWalk("test", "test", "test")

    then:
    null != val
    1 == val.size()
    val.get(0) instanceof TestObject

  }

  def "Test link walking as type"() {

    when:
    def val = riak.linkWalkAsType("test", "test", "test", Map)

    then:
    null != val
    1 == val.size()
    val.get(0) instanceof Map

  }

  def "Test getAndSet with Map"() {

    given:
    def i = run++
    def newObj = [test: "value $i".toString(), integer: 12]

    when:
    def oldObj = riak.getAndSet("test", "test", newObj)

    then:
    "value" == oldObj.test

  }

  def "Test Map/Reduce returning Integer"() {

    given:
    MapReduceJob job = new RiakMapReduceJob(riak)
    def uuid = UUID.randomUUID().toString()
    def mapJs = new JavascriptMapReduceOperation("function(v){ var uuid='$uuid'; ejsLog('/tmp/mapred.log', 'map input: '+JSON.stringify(v)); var o=Riak.mapValuesJson(v); return [1]; }")
    def mapPhase = new RiakMapReducePhase("map", "javascript", mapJs)

    def reduceJs = new JavascriptMapReduceOperation("function(v){ var uuid='$uuid'; ejsLog('/tmp/mapred.log', 'reduce input: '+JSON.stringify(v)); var s=Riak.reduceSum(v); ejsLog('/tmp/mapred.log', 'reduce output: '+JSON.stringify(s)); return s; }")
    def reducePhase = new RiakMapReducePhase("reduce", "javascript", reduceJs)

    job.addInputs(["test"]).
        addPhase(mapPhase).
        addPhase(reducePhase)
    println job.toJson()

    when:
    def result = riak.execute(job, Integer)

    then:
    2 == result

  }

  def "Test Map/Reduce returning List"() {

    given:
    MapReduceJob job = new RiakMapReduceJob(riak)
    def uuid = UUID.randomUUID().toString()
    def mapJs = new JavascriptMapReduceOperation("function(v){ var uuid='$uuid'; ejsLog('/tmp/mapred.log', 'map input: '+JSON.stringify(v)); var o=Riak.mapValuesJson(v); return [1]; }")
    def mapPhase = new RiakMapReducePhase("map", "javascript", mapJs)

    def reduceJs = new JavascriptMapReduceOperation("function(v){ var uuid='$uuid'; ejsLog('/tmp/mapred.log', 'reduce input: '+JSON.stringify(v)); var s=Riak.reduceSum(v); ejsLog('/tmp/mapred.log', 'reduce output: '+JSON.stringify(s)); return s; }")
    def reducePhase = new RiakMapReducePhase("reduce", "javascript", reduceJs)

    job.addInputs(["test"]).
        addPhase(mapPhase).
        addPhase(reducePhase)

    when:
    def result = riak.execute(job)

    then:
    1 == result.size()
    2 == result[0]

  }

  def "Test RiakFile"() {

    given:
    def file = new RiakFile(riak, "test", "test")

    when:
    def exists = file.exists()

    then:
    exists

    when:
    def content = file.toURI().toURL().openConnection().getContent()

    then:
    null != content

  }

  def "Test delete key"() {

    when:
    def deleted = riak.deleteKeys("test:test", "${TestObject.name}:test", "test:$id")

    then:
    true == deleted

  }

}