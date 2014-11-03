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

import java.util.List;

/**
 * @author David Liu
 * @since 1.2
 *
 */
public class TargetMethod {

	private String methodName;
	private List<Class> typeList;
	private List<Object> valueList;

	/**
	 * @return the methodName
	 */
	public String getMethodName() {
		return methodName;
	}

	/**
	 * @param methodName
	 *            the methodName to set
	 */
	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}

	/**
	 * @return the typeList
	 */
	public Class[] getTypeList() {
		return typeList.toArray(new Class[0]);
	}

	/**
	 * @param typeList
	 *            the typeList to set
	 */
	public void setTypeList(List<Class> typeList) {
		this.typeList = typeList;
	}

	/**
	 * @return the valueList
	 */
	public List<Object> getValueList() {
		return valueList;
	}

	/**
	 * @param valueList
	 *            the valueList to set
	 */
	public void setValueList(List<Object> valueList) {
		this.valueList = valueList;
	}

	public TargetMethod(String methodName, List<Class> typeList,
			List<Object> valueList) {
		this.methodName = methodName;
		this.typeList = typeList;
		this.valueList = valueList;
	}

}
