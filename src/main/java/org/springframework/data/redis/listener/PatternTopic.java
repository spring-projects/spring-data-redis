/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.redis.listener;

import org.springframework.util.Assert;

/**
 * {@link Topic} {@link String pattern} matching multiple Redis channels.
 *
 * @author Costin Leau
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class PatternTopic extends AbstractTopic {

	/**
	 * Constructs a new {@link PatternTopic} instance.
	 *
	 * @param channelPattern must not be {@literal null}.
	 */
	public PatternTopic(String channelPattern) {

		super(channelPattern);
		Assert.notNull(channelPattern, "Pattern must not be null");
	}

	/**
	 * Create a new {@link PatternTopic} for channel subscriptions based on a {@code pattern}.
	 *
	 * @param pattern {@link String pattern} used to match channels; must not be {@literal null} or empty.
	 * @return the {@link PatternTopic} for the given {@code pattern}.
	 * @since 2.1
	 */
	public static PatternTopic of(String pattern) {
		return new PatternTopic(pattern);
	}
}
