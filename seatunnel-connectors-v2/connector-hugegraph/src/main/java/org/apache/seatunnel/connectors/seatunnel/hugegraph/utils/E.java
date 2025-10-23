package org.apache.seatunnel.connectors.seatunnel.hugegraph.utils;

import javax.annotation.Nullable;

import java.util.Collection;

public final class E {

    public static void checkNotNull(Object object, String elem) {
        if (object == null) {
            throw new NullPointerException(String.format("The '%s' can't be null", elem));
        }
    }

    public static void checkNotNull(Object object, String elem, String owner) {
        if (object == null) {
            throw new NullPointerException(
                    String.format("The '%s' of '%s' can't be null", elem, owner));
        }
    }

    public static void checkNotEmpty(Collection<?> collection, String elem) {
        if (collection == null || collection.isEmpty()) {
            throw new IllegalArgumentException(String.format("The '%s' can't be empty", elem));
        }
    }

    public static void checkNotEmpty(Collection<?> collection, String elem, String owner) {
        if (collection == null || collection.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("The '%s' of '%s' can't be empty", elem, owner));
        }
    }

    public static void checkArgument(
            boolean expression, @Nullable String message, @Nullable Object... args) {
        if (!expression) {
            String formattedMessage = (message == null) ? "" : String.format(message, args);
            throw new IllegalArgumentException(formattedMessage);
        }
    }

    public static void checkArgumentNotNull(
            Object object, @Nullable String message, @Nullable Object... args) {
        checkArgument(object != null, message, args);
    }

    public static void checkState(
            boolean expression, @Nullable String message, @Nullable Object... args) {
        if (!expression) {
            String formattedMessage = (message == null) ? "" : String.format(message, args);
            throw new IllegalStateException(formattedMessage);
        }
    }
}
