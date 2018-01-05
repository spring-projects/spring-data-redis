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
 * {@link ClusterRedirectException} indicates that a requested slot is not served by the targeted server but can be
 * obtained on another one.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class ClusterRedirectException extends DataRetrievalFailureException {

	private static final long serialVersionUID = -857075813794333965L;

	private final int slot;
	private final String host;
	private final int port;

	/**
	 * Creates new {@link ClusterRedirectException}.
	 *
	 * @param slot the slot to redirect to.
	 * @param targetHost the host to redirect to.
	 * @param targetPort the port on the host.
	 * @param e the root cause from the data access API in use.
	 */
	public ClusterRedirectException(int slot, String targetHost, int targetPort, Throwable e) {

		super(String.format("Redirect: slot %s to %s:%s.", slot, targetHost, targetPort), e);

		this.slot = slot;
		this.host = targetHost;
		this.port = targetPort;
	}

	/**
	 * @return the slot to go for.
	 */
	public int getSlot() {
		return slot;
	}

	/**
	 * @return host serving the slot.
	 */
	public String getTargetHost() {
		return host;
	}

	/**
	 * @return port on host serving the slot.
	 */
	public int getTargetPort() {
		return port;
	}

}
