/*
 * Copyright (c) 2011 by J. Brisbin <jon@jbrisbin.com>
 * Portions (c) 2011 by NPC International, Inc. or the
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

import org.springframework.data.keyvalue.riak.util.RiakClassFileLoader
import org.springframework.data.keyvalue.riak.util.RiakClassLoader
import spock.lang.Shared
import spock.lang.Specification

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
class RiakClassLoaderSpec extends Specification {

  @Shared RiakTemplate riakTemplate = new RiakTemplate()

  def setupSpec() {
    RiakQosParameters qos = new RiakQosParameters()
    qos.durableWriteThreshold = "all"
    riakTemplate.defaultQosParameters = qos
    riakTemplate.ignoreNotFound = true
    riakTemplate.afterPropertiesSet()
  }

  def "Test load class file into Riak"() {

    when:
    def args = [
        "-c", "src/test/classes/org/springframework/data/keyvalue/riak/core/ClassLoaderTest.class",
        "-b", "test",
        "-k", "org.springframework.data.keyvalue.riak.core.ClassLoaderTest"
    ].toArray(new String[6])
    RiakClassFileLoader.main(args)

    then:
    true

  }

  def "Test find class previously loaded into Riak"() {

    given:
    RiakClassLoader classLoader = new RiakClassLoader(riakTemplate)
    classLoader.defaultBucket = "test"

    when:
    def clazz = Class.forName("org.springframework.data.keyvalue.riak.core.ClassLoaderTest", false, classLoader)
    def inst = clazz?.newInstance()

    then:
    null != clazz
    null != inst
    inst.name == "ClassLoaderTest"

  }

}
