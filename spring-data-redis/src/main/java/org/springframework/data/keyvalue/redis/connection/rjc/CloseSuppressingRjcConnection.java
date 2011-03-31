/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.connection.rjc;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import org.idevlab.rjc.ds.RedisConnection;
import org.idevlab.rjc.message.RedisNodeSubscriber;
import org.idevlab.rjc.protocol.Protocol.Command;

/**
 * Basic decorator suppressing close() calls to the underlying connection.
 * Used for reusing arbitrary connections with {@link RedisNodeSubscriber} without
 * resorting to connection pooling.
 * 
 * @author Costin Leau
 */
class CloseSuppressingRjcConnection implements RedisConnection {

	private final RedisConnection delegate;

	/**
	 * Constructs a new <code>CloseSuppressingRjcConnection</code> instance.
	 *
	 * @param delegate
	 */
	CloseSuppressingRjcConnection(RedisConnection delegate) {
		this.delegate = delegate;
	}

	public void close() {
		// no-op
	}

	public void connect() throws UnknownHostException, IOException {
		delegate.connect();
	}

	public List<Object> getAll() {
		return delegate.getAll();
	}

	public byte[] getBinaryBulkReply() {
		return delegate.getBinaryBulkReply();
	}

	public List<byte[]> getBinaryMultiBulkReply() {
		return delegate.getBinaryMultiBulkReply();
	}

	public String getBulkReply() {
		return delegate.getBulkReply();
	}

	public String getHost() {
		return delegate.getHost();
	}

	public Long getIntegerReply() {
		return delegate.getIntegerReply();
	}

	public List<String> getMultiBulkReply() {
		return delegate.getMultiBulkReply();
	}

	public List<Object> getObjectMultiBulkReply() {
		return delegate.getObjectMultiBulkReply();
	}

	public Object getOne() {
		return delegate.getOne();
	}

	public int getPort() {
		return delegate.getPort();
	}

	public String getStatusCodeReply() {
		return delegate.getStatusCodeReply();
	}

	public int getTimeout() {
		return delegate.getTimeout();
	}

	public boolean isConnected() {
		return delegate.isConnected();
	}

	public void rollbackTimeout() {
		delegate.rollbackTimeout();
	}

	public void sendCommand(Command arg0, byte[]... arg1) {
		delegate.sendCommand(arg0, arg1);
	}

	public void sendCommand(Command arg0, String... arg1) {
		delegate.sendCommand(arg0, arg1);
	}

	public void sendCommand(Command arg0) {
		delegate.sendCommand(arg0);
	}

	public void setTimeoutInfinite() {
		delegate.setTimeoutInfinite();
	}
}