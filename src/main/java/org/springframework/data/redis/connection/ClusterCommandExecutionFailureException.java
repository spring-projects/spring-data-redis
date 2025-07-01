/*
 * Copyright 2015-2025 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.io.Serial;
import java.util.Collections;
import java.util.List;

import org.springframework.dao.UncategorizedDataAccessException;

/**
 * Exception thrown when at least one call to a clustered redis environment fails.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class ClusterCommandExecutionFailureException extends UncategorizedDataAccessException {

	private static final @Serial long serialVersionUID = 5727044227040368955L;

	/**
	 * Creates new {@link ClusterCommandExecutionFailureException}.
	 *
	 * @param cause must not be {@literal null}.
	 */
	public ClusterCommandExecutionFailureException(Throwable cause) {
		this(Collections.singletonList(cause));
	}

	/**
	 * Creates new {@link ClusterCommandExecutionFailureException}.
	 *
	 * @param causes must not be {@literal empty}.
	 */
	public ClusterCommandExecutionFailureException(List<? extends Throwable> causes) {

		super(causes.get(0).getMessage(), causes.get(0));

		causes.forEach(this::addSuppressed);
	}

}
