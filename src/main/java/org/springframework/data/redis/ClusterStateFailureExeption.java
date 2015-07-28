/*
 * Copyright 2015 the original author or authors.
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

import org.springframework.dao.DataAccessResourceFailureException;

/**
 * {@link DataAccessResourceFailureException} indicating the current local snapshot of cluster state does no longer
 * represent the actual remote state. This can happen nodes are removed from cluster, slots get migrated to other nodes
 * and so on.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class ClusterStateFailureExeption extends DataAccessResourceFailureException {

	private static final long serialVersionUID = 333399051713240852L;

	/**
	 * Creates new {@link ClusterStateFailureExeption}.
	 * 
	 * @param msg
	 */
	public ClusterStateFailureExeption(String msg) {
		super(msg);
	}

	/**
	 * Creates new {@link ClusterStateFailureExeption}.
	 * 
	 * @param msg
	 * @param cause
	 */
	public ClusterStateFailureExeption(String msg, Throwable cause) {
		super(msg, cause);
	}

}
