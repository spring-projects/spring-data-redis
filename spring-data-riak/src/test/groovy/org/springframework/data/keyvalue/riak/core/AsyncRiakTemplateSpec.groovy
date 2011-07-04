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

import java.util.concurrent.Future
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@ContextConfiguration(locations = "/org/springframework/data/AsyncRiakTemplateTests.xml")
class AsyncRiakTemplateSpec extends Specification {

  @Autowired
  ApplicationContext appCtx
  @Autowired
  AsyncRiakTemplate riak

  def "Test async setWithMetaData"() {

    given:
    def obj = [test: "value", integer: 12]
    def success = false
    def failure = false
    def testValue = "bad value"
    def callback = [
        completed: { v ->
          success = true
          testValue = v.get().test
        },
        failed: { e ->
          failure = true
        }
    ] as AsyncKeyValueStoreOperation

    when:
    Future future = riak.setWithMetaData("test", "test", obj, null, null, callback)
    println "Waiting for result: ${future.get()}"

    then:
    success && !failure
    "value" == testValue

  }

  def "Test async getWithMetaData"() {

    given:
    def result = null
    def callback = [
        completed: { meta, v ->
          println "got value: $meta $v"
          result = v
        },
        failed: { e ->
          println "got error: $e"
        }
    ] as AsyncKeyValueStoreOperation

    when:
    riak.getWithMetaData("test", "test", Map, callback).get()

    then:
    null != result

  }

}
