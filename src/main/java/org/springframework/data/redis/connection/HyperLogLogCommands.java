/*
 * Copyright 2014 the original author or authors.
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

/**
 * {@literal HyperLogLog} specific commands supported by Redis.
 * 
 * @author Christoph Strobl
 * @since 1.5
 */
public interface HyperLogLogCommands {

	/**
	 * Adds given {@literal values} to the HyperLogLog stored at given {@literal key}.
	 * 
	 * @param key
	 * @param values
	 * @return
	 */
	Long pfAdd(byte[] key, byte[]... values);

	/**
	 * Return the approximated cardinality of the structures observed by the HyperLogLog at {@literal key(s)}.
	 * 
	 * @param keys
	 * @return
	 */
	Long pfCount(byte[]... keys);

	/**
	 * Merge N different HyperLogLogs at {@literal sourceKeys} into a single {@literal destinationKey}.
	 * 
	 * @param destinationKey
	 * @param sourceKeys
	 */
	void pfMerge(byte[] destinationKey, byte[]... sourceKeys);

}
