/*
 * Copyright 2012-2014 the original author or authors.
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
 * @author Christoph Strobl
 * @author David Liu
 */
public interface RedisScriptingCommands {

	/**
	 * Flush lua script cache.
	 * 
	 * @see http://redis.io/commands/script-flush
	 */
	void scriptFlush();

	/**
	 * Kill current lua script execution.
	 * 
	 * @see http://redis.io/commands/script-kill
	 */
	void scriptKill();

	/**
	 * Load lua script into scripts cache, without executing it.<br />
	 * Execute the script by calling {@link #evalSha(String, ReturnType, int, byte[])}.
	 * 
	 * @see http://redis.io/commands/script-load
	 * @param script
	 * @return
	 */
	String scriptLoad(byte[] script);

	/**
	 * Check if given {@code scriptShas} exist in script cache.
	 * 
	 * @see http://redis.io/commands/script-exits
	 * @param scriptShas
	 * @return one entry per given scriptSha in returned list.
	 */
	List<Boolean> scriptExists(String... scriptShas);

	/**
	 * Evaluate given {@code script}.
	 * 
	 * @see http://redis.io/commands/eval
	 * @param script
	 * @param returnType
	 * @param numKeys
	 * @param keysAndArgs
	 * @return
	 */
	<T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs);

	/**
	 * Evaluate given {@code scriptSha}.
	 * 
	 * @see http://redis.io/commands/evalsha
	 * @param script
	 * @param returnType
	 * @param numKeys
	 * @param keysAndArgs
	 * @return
	 */
	<T> T evalSha(String scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs);

	/**
	 * Evaluate given {@code scriptSha}.
	 * 
	 * @see http://redis.io/commands/evalsha
	 * @param script
	 * @param returnType
	 * @param numKeys
	 * @param keysAndArgs
	 * @return
	 * @since 1.5
	 */
	<T> T evalSha(byte[] scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs);
}
