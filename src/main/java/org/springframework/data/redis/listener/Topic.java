/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.listener;

/**
 * Topic for a Redis message. Acts a high-level abstraction on top of Redis low-level channels or patterns.
 *
 * @author Costin Leau
 */
public interface Topic {

	/**
	 * Returns the topic (as a String).
	 *
	 * @return the topic
	 */
	String getTopic();
}
