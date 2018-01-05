/*
 * Copyright 2015-2018 the original author or authors.
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
package org.springframework.data.redis;

import org.springframework.dao.DataRetrievalFailureException;

/**
 * {@link DataRetrievalFailureException} thrown when following cluster redirects exceeds the max number of edges.
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public class TooManyClusterRedirectionsException extends DataRetrievalFailureException {

	private static final long serialVersionUID = -2818933672669154328L;

	/**
	 * Creates new {@link TooManyClusterRedirectionsException}.
	 *
	 * @param msg the detail message.
	 */
	public TooManyClusterRedirectionsException(String msg) {
		super(msg);
	}

	/**
	 * Creates new {@link TooManyClusterRedirectionsException}.
	 *
	 * @param msg the detail message.
	 * @param cause the root cause from the data access API in use.
	 */
	public TooManyClusterRedirectionsException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
