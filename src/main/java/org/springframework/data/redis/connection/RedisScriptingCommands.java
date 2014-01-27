/*
 * Copyright 2012-2013 the original author or authors.
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

package org.springframework.data.redis.connection;

import java.util.List;

/**
 * Scripting commands.
 * 
 * @author Costin Leau
 */
public interface RedisScriptingCommands {

	void scriptFlush();

	void scriptKill();

	String scriptLoad(byte[] script);

	List<Boolean> scriptExists(String... scriptSha1);

	<T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs);

	<T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs);
}
