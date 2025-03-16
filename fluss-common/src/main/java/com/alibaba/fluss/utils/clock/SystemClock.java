/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.utils.clock;

import com.alibaba.fluss.annotation.PublicEvolving;

/**
 * A clock that returns the time of the system / process.
 *
 * <p>This clock uses {@link System#currentTimeMillis()} for <i>milliseconds</i> and {@link
 * System#nanoTime()} for <i>nanoseconds</i>.
 *
 * <p>This SystemClock exists as a singleton instance.
 *
 * @since 0.2
 */
@PublicEvolving
public final class SystemClock implements Clock {

    private static final SystemClock INSTANCE = new SystemClock();

    public static SystemClock getInstance() {
        return INSTANCE;
    }

    // ------------------------------------------------------------------------

    private SystemClock() {}

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
        return System.nanoTime();
    }
}
