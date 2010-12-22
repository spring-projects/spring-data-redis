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

package org.springframework.data.keyvalue.riak.core

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.data.keyvalue.riak.groovy.RiakBuilder
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@ContextConfiguration(locations = "/org/springframework/data/AsyncRiakTemplateTests.xml")
class RiakBuilderSpec extends Specification {

  @Autowired
  ApplicationContext appCtx
  @Autowired
  AsyncRiakTemplate riakTemplate

  def "Test builder set"() {

    given:
    def obj = [test: "value", integer: 12]
    def riak = new RiakBuilder(riakTemplate)
    def result = null

    when:
    riak {
      set(bucket: "test", key: "test", qos: [dw: "all"], value: obj) {
        completed(when: { it.integer == 12 }) { result = it.test }
        completed { result = "otherwise" }
        failed { it.printStackTrace() }
      }
    }

    then:
    "value" == result

  }

  def "Test builder get"() {

    given:
    def riak = new RiakBuilder(riakTemplate)
    def result = null

    when:
    riak.get(bucket: "test", key: "test") {
      completed(when: { it.integer == 12 }) { result = it.test }
      completed { result = "otherwise" }
      failed { it.printStackTrace() }
    }

    then:
    "value" == result

  }

  def "Test builder getAsType"() {

    given:
    def riak = new RiakBuilder(riakTemplate)
    def result = null

    when:
    riak.getAsType(bucket: "test", key: "test", type: Map) {
      completed(when: { it instanceof Map }) { result = it.test }
      completed { result = "otherwise" }
      failed { it.printStackTrace() }
    }

    then:
    "value" == result

  }

  def "Test builder setAsBytes"() {

    given:
    def obj = "test bytes".bytes
    def riak = new RiakBuilder(riakTemplate)
    def result = null

    when:
    riak.setAsBytes(bucket: "test", key: "test", value: obj, qos: [dw: "all"]) {
      completed { result = "success" }
      failed { it.printStackTrace() }
    }

    then:
    null != result
    "success" == result

  }

  def "Test builder get with bytes"() {

    given:
    def riak = new RiakBuilder(riakTemplate)
    def result = null

    when:
    riak.get(bucket: "test", key: "test") {
      completed { result = it }
      failed { it.printStackTrace() }
    }

    then:
    null != result
    "test bytes".bytes == result

  }

  def "Test builder put"() {

    given:
    def obj = [test: "value", integer: 12]
    def riak = new RiakBuilder(riakTemplate)
    def id = null

    when:
    riak.put(bucket: "test", qos: [dw: "all"], value: obj) {
      completed { v, meta -> id = meta.key }
      failed { it.printStackTrace() }
    }

    then:
    null != id

  }

  def "Test builder foreach"() {

    given:
    def riak = new RiakBuilder(riakTemplate)
    def idCnt = 0

    when:
    riak.foreach(bucket: "test") {
      completed { idCnt++ }
      failed { it.printStackTrace() }
    }

    then:
    idCnt > 0

  }

  def "Test builder batch operations"() {

    given:
    def riak = new RiakBuilder(riakTemplate)
    def ids = []

    when:
    riak {
      put(bucket: "test", value: [test: "value 1"])
      put(bucket: "test", value: [test: "value 2"])
      put(bucket: "test", value: [test: "value 3"])

      foreach(bucket: "test") {
        completed { v, meta -> ids << meta.key }
        failed { it.printStackTrace() }
      }
    }

    then:
    null != ids
    3 <= ids.size()

  }

  def "Test builder bucket as node"() {

    given:
    def riak = new RiakBuilder(riakTemplate)
    def ids = []

    when:
    riak {
      "test" {
        put(value: [test: "value 1"])
        put(value: [test: "value 2"])
        put(value: [test: "value 3"])

        foreach {
          completed { v, meta -> ids << meta.key }
          failed { it.printStackTrace() }
        }
      }
    }

    then:
    null != ids
    3 <= ids.size()

  }

  def "Test builder Map/Reduce"() {

    given:
    def riak = new RiakBuilder(riakTemplate)
    def result = []

    when:
    riak {
      mapreduce {
        inputs "test"
        query {
          map(arg: [test: "arg", alist: [1, 2, 3, 4]]) {
            source "function(v, keyInfo, arg){ ejsLog('/tmp/mapred.log', JSON.stringify(v)); ejsLog('/tmp/mapred.log', JSON.stringify(keyInfo)); ejsLog('/tmp/mapred.log', JSON.stringify(arg)); return [1]; }"
          }
          reduce {
            source "function(v){ ejsLog('/tmp/mapred.log', JSON.stringify(arguments)); return Riak.reduceSum(v); }"
          }
        }
        completed { result = it }
        failed { it.printStackTrace() }
      }
    }

    then:
    null != result
    1 <= result.size()

  }

  def "Test builder delete"() {

    given:
    def riak = new RiakBuilder(riakTemplate)
    def deleted = false

    when:
    riak {
      "test" {
        foreach {
          completed { v, meta ->
            delete(bucket: meta.bucket, key: meta.key) {
              completed { deleted = true }
              failed { deleted = false }
            }
          }
          failed { it.printStackTrace() }
        }
      }
    }

    then:
    deleted

  }

}
