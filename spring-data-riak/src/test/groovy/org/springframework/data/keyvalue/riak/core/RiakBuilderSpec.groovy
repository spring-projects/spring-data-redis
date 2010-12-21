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
    riak.set(bucket: "test", key: "test", qos: [dw: "all"], value: obj) {

      completed(when: { v -> v.integer == 12 }) { v, meta ->
        result = v.test
      }
      completed { v -> result = "otherwise" }

      failed { e -> println "failure: $e" }

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

      completed(when: { v -> v.integer == 12 }) { v, meta ->
        result = v.test
      }
      completed { v -> result = "otherwise" }

      failed { e -> println "failure: $e" }

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
      completed { v -> result = "success" }
      failed { e -> result = "failure" }
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
    riak.get(bucket: "test", key: "test", wait: 3000L) {
      completed { v -> result = v }
      failed { e -> println "failure: $e" }
    }

    then:
    null != result
    "test bytes".bytes == result

  }

  def "Test builder delete"() {

    given:
    def riak = new RiakBuilder(riakTemplate)
    def result = null

    when:
    riak.delete(bucket: "test", key: "test", wait: 3000L) {
      completed { v -> result = v }
      failed { e -> println "failure: $e" }
    }

    then:
    null != result
    result

  }

}
