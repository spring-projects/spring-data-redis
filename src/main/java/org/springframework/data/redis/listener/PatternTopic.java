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

import lombok.EqualsAndHashCode;

import org.springframework.util.Assert;

/**
 * Pattern topic (matching multiple channels).
 *
 * @author Costin Leau
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@EqualsAndHashCode
public class PatternTopic implements Topic {

	private final String channelPattern;

	/**
	 * Constructs a new {@link PatternTopic} instance.
	 *
	 * @param pattern must not be {@literal null}.
	 */
	public PatternTopic(String pattern) {

		Assert.notNull(pattern, "Pattern must not be null!");

		this.channelPattern = pattern;
	}

	/**
	 * Create a new {@link PatternTopic} for channel subscriptions based on a {@code pattern}.
	 *
	 * @param pattern the channel pattern, must not be {@literal null} or empty.
	 * @return the {@link PatternTopic} for {@code pattern}.
	 * @since 2.1
	 */
	public static PatternTopic of(String pattern) {
		return new PatternTopic(pattern);
	}

	/**
	 * @return channel pattern.
	 */
	@Override
	public String getTopic() {
		return channelPattern;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return channelPattern;
	}
}
