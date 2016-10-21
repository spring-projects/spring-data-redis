/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.data.redis.test.util;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AssertProvider;
import org.springframework.data.redis.core.convert.Bucket;

/**
 * @author Mark Paluch
 */
public class BucketTester implements AssertProvider<BucketTester.BucketAssert> {

    private Bucket bucket;

    private BucketTester(Bucket bucket) {
        this.bucket = bucket;
    }

    public static BucketTester from(Bucket bucket) {
        return new BucketTester(bucket);
    }

    @Override
    public BucketAssert assertThat() {
        return new BucketAssert(bucket);
    }

    public static class BucketAssert extends AbstractAssert<BucketAssert, Bucket> {

        protected BucketAssert(Bucket actual) {
            super(actual, BucketAssert.class);
        }

        /**
         * Checks for presence of type hint at given path.
         *
         * @param path
         * @param type
         * @return
         */
        public BucketAssert containingTypeHint(String path, Class<?> type) {
            return containingUtf8String(path, type.getName());
        }

        /**
         * Checks for presence of equivalent String value at path.
         *
         * @param path
         * @param value
         * @return
         */
        public BucketAssert containingUtf8String(String path, String value) {

            byte[] actualPathValue = actual.get(path);
            if ((actualPathValue != null && value == null) || (actualPathValue == null && value != null)
                    || (!value.equals(new String(actualPathValue, StandardCharsets.UTF_8)))) {
                throw new AssertionError(String.format("Expected %s to contain path %s with value %s but was %s", actual, path,
                        value, actual.get(path)));
            }

            return this;
        }

        /**
         * Checks for presence of given value at path.
         *
         * @param path
         * @param value
         * @return
         */
        public BucketAssert containing(String path, byte[] value) {

            byte[] actualPathValue = actual.get(path);
            if ((actualPathValue != null && value == null) || (actualPathValue == null && value != null)
                    || !Arrays.equals(value, actualPathValue)) {
                throw new AssertionError(String.format("Expected %s to contain path %s with hint %s but was %s", actual, path,
                        Arrays.toString(value), Arrays.toString(actual.get(path))));
            }
            return this;
        }

        /**
         * Checks for presence of given value at path.
         *
         * @param path
         * @param value
         * @return
         */
        public BucketAssert without(String path) {

            byte[] actualPathValue = actual.get(path);
            if (actualPathValue != null) {
                throw new AssertionError(String.format("Expected %s to not contain path %s but was %s", actual, path,
                        Arrays.toString(actualPathValue)));
            }
            return this;
        }

        /**
         * Checks for presence of equivalent time in msec value at path.
         *
         * @param path
         * @param date
         * @return
         */
        public BucketAssert containingDateAsMsec(String path, Date date) {
            return containingUtf8String(path, "" + date.getTime());
        }

    }
}
