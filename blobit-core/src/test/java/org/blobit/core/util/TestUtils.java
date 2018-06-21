/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package org.blobit.core.util;

import java.util.concurrent.Callable;

/**
 *
 * @author francesco.caliumi
 */
public class TestUtils {

    public static final double WAIT_FOR_CONDITION_MULTIPLIER = Integer.getInteger("magnews.tests.waitforcondition.multiplier", 1);

    /**
     * ** Stolen from JUnit 4.13 ** *
     */
    public interface ThrowingRunnable {

        void run() throws Throwable;
    }

    /**
     * Asserts that {@code runnable} throws an exception of type {@code expectedThrowable} when executed. If it does not
     * throw an exception, an {@link AssertionError} is thrown. If it throws the wrong type of exception, an
     * {@code AssertionError} is thrown describing the mismatch; the exception that was actually thrown can be obtained
     * by calling {@link
     * AssertionError#getCause}.
     *
     * @param expectedThrowable the expected type of the exception
     * @param runnable a function that is expected to throw an exception when executed
     * @since 4.13
     */
    public static void assertThrows(Class<? extends Throwable> expectedThrowable, ThrowingRunnable runnable) {
        expectThrows(expectedThrowable, runnable);
    }

    /**
     * Asserts that {@code runnable} throws an exception of type {@code expectedThrowable} when executed. If it does,
     * the exception object is returned. If it does not throw an exception, an {@link AssertionError} is thrown. If it
     * throws the wrong type of exception, an {@code
     * AssertionError} is thrown describing the mismatch; the exception that was actually thrown can be obtained by
     * calling {@link AssertionError#getCause}.
     *
     * @param expectedThrowable the expected type of the exception
     * @param runnable a function that is expected to throw an exception when executed
     * @return the exception thrown by {@code runnable}
     * @since 4.13
     */
    public static <T extends Throwable> T expectThrows(Class<T> expectedThrowable, ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable actualThrown) {
            if (expectedThrowable.isInstance(actualThrown)) {
                @SuppressWarnings("unchecked")
                T retVal = (T) actualThrown;
                return retVal;
            } else {
                actualThrown.printStackTrace();
                String mismatchMessage = String.format("unexpected exception type thrown expected %s actual %s",
                        expectedThrowable.getSimpleName(), actualThrown.getClass().getSimpleName());

                // The AssertionError(String, Throwable) ctor is only available on JDK7.
                AssertionError assertionError = new AssertionError(mismatchMessage);
                assertionError.initCause(actualThrown);
                throw assertionError;
            }
        }
        String message = String.format("expected %s to be thrown, but nothing was thrown",
                expectedThrowable.getSimpleName());
        throw new AssertionError(message);
    }

    public static void waitForCondition(Callable<Boolean> condition, Callable<Void> callback, int seconds) throws Exception {
        try {
            long _start = System.currentTimeMillis();
            long millis = (long) (seconds * 1000 * WAIT_FOR_CONDITION_MULTIPLIER);
            while (System.currentTimeMillis() - _start <= millis) {
                if (condition.call()) {
                    return;
                }
                callback.call();
                Thread.sleep(100);
            }
        } catch (InterruptedException ee) {
            ee.printStackTrace();
            throw new AssertionError("test interrupted!");
        } catch (Exception ee) {
            ee.printStackTrace();
            throw new AssertionError("error while evaluating condition:" + ee, ee);
        }
        throw new AssertionError("condition not met in time!");
    }

    public static Callable<Void> NOOP = new Callable<Void>() {
        @Override
        public Void call() {
            return null;
        }
    };
}
