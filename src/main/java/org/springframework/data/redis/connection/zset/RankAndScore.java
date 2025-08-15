/*
 * Copyright 2011-2025 the original author or authors.
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
package org.springframework.data.redis.connection.zset;

import org.jspecify.annotations.Nullable;

/**
 * ZSet rank with score.
 * @author Seongil Kim
 */
public class RankAndScore {
    private final Long rank;
    private final Double score;

    public RankAndScore(Long rank, Double score) {
        this.rank = rank;
        this.score = score;
    }

    public Long getRank() {
        return rank;
    }

    public Double getScore() {
        return score;
    }

    public boolean equals(@Nullable Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof RankAndScore other))
            return false;
        if (!score.equals(other.score))
            return false;
        if (!rank.equals(other.rank))
            return false;
        return true;
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + score.hashCode();
        result = prime * result + rank.hashCode();
        return result;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [rank=").append(rank);
        sb.append(", score=").append(score);
        sb.append(']');
        return sb.toString();
    }
}
