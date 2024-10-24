package com.alicloud.tablestore.common;

import junit.framework.AssertionFailedError;

import static org.junit.Assert.assertTrue;

public class TestUtils {
    public static <T extends Throwable> T expectThrowsAndMessages(Class<T> expectedType, Runnable runnable, String... messageKeys) {
        try {
            runnable.run();
        } catch (Throwable e) {
            if (expectedType.isInstance(e)) {
                String message = e.getMessage();
                for (String key : messageKeys) {
                    assertTrue("message key[" + key + "] not in message:" + message, message.contains(key));
                }
                return expectedType.cast(e);
            }
            AssertionFailedError assertion = new AssertionFailedError("Unexpected exception type, expected " + expectedType.getSimpleName() + " but got " + e);
            assertion.initCause(e);
            throw assertion;
        }
        throw new AssertionFailedError("Expected exception " + expectedType.getSimpleName() + " but no exception was thrown");
    }

    public static <TO extends Throwable, TW extends Throwable> TW expectThrowsAndMessages(
            Class<TO> expectedOuterType,
            Class<TW> expectedWrappedType,
            Runnable runnable,
            String... messageKeys) {
        try {
            runnable.run();
        } catch (Throwable e) {
            if (expectedOuterType.isInstance(e)) {
                Throwable cause = e.getCause();
                if (expectedWrappedType.isInstance(cause)) {
                    String message = e.getMessage();
                    for (String key : messageKeys) {
                        assertTrue("message key[" + key + "] not in message:" + message, message.contains(key));
                    }
                    return expectedWrappedType.cast(cause);
                } else {
                    AssertionFailedError assertion = new AssertionFailedError("Unexpected wrapped exception type, expected "
                            + expectedWrappedType.getSimpleName() + ", but is " + cause.getClass().getSimpleName());
                    assertion.initCause(e);
                    throw assertion;
                }
            }
            AssertionFailedError assertion = new AssertionFailedError("Unexpected outer exception type, expected "
                    + expectedOuterType.getSimpleName() + ", but is " + e.getClass().getSimpleName());
            assertion.initCause(e);
            throw assertion;
        }
        throw new AssertionFailedError("Expected outer exception " + expectedOuterType.getSimpleName());
    }
}
