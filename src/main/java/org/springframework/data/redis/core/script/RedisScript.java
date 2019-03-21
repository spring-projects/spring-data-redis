/*
 * Copyright 2013-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core.script;

/**
 * A script to be executed using the <a href="http://redis.io/commands/eval">Redis scripting support</a> available as of
 * version 2.6
 * 
 * @author Jennifer Hickey
 * @param <T> The script result type. Should be one of Long, Boolean, List, or deserialized value type. Can be null if
 *          the script returns a throw-away status (i.e "OK")
 */
public interface RedisScript<T> {

	/**
	 * @return The SHA1 of the script, used for executing Redis evalsha command
	 */
	String getSha1();

	/**
	 * @return The script result type. Should be one of Long, Boolean, List, or deserialized value type. Can be null if
	 *         the script returns a throw-away status (i.e "OK")
	 */
	Class<T> getResultType();

	/**
	 * @return The script contents
	 */
	String getScriptAsString();

}
