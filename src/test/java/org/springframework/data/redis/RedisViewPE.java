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
package org.springframework.data.redis;

import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;

/**
 * @author Costin Leau
 */
public class RedisViewPE {

	private ValueOperations<String, String> valueOps;
	private ListOperations<String, String> listOps;
	private SetOperations<String, String> setOps;
	private ZSetOperations<String, Object> zsetOps;
	private HashOperations<Object, String, Object> hashOps;

	public ValueOperations<String, String> getValueOps() {
		return valueOps;
	}

	public void setValueOps(ValueOperations<String, String> valueOps) {
		this.valueOps = valueOps;
	}

	public ListOperations<String, String> getListOps() {
		return listOps;
	}

	public void setListOps(ListOperations<String, String> listOps) {
		this.listOps = listOps;
	}

	public SetOperations<String, String> getSetOps() {
		return setOps;
	}

	public void setSetOps(SetOperations<String, String> setOps) {
		this.setOps = setOps;
	}

	public ZSetOperations<String, Object> getZsetOps() {
		return zsetOps;
	}

	public void setZsetOps(ZSetOperations<String, Object> zsetOps) {
		this.zsetOps = zsetOps;
	}

	public HashOperations<Object, String, Object> getHashOps() {
		return hashOps;
	}

	public void setHashOps(HashOperations<Object, String, Object> hashOps) {
		this.hashOps = hashOps;
	}
}
