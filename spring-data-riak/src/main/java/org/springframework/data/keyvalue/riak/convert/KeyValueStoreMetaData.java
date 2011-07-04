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

package org.springframework.data.keyvalue.riak.convert;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Specify the bucket in which to store the annotated object, overriding the
 * default classname method of deriving bucket name.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface KeyValueStoreMetaData {

  /**
   * The bucket in which to store an instance of this object.
   *
   * @return
   */
  String bucket();

  /**
   * The media type in which to covert and store this object.
   *
   * @return
   */
  String mediaType() default "application/json";

}
