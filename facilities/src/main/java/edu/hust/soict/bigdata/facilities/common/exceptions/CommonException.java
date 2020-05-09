package edu.hust.soict.bigdata.facilities.common.exceptions;


import edu.hust.soict.bigdata.facilities.common.util.Strings;

import java.util.Arrays;
import java.util.stream.Collectors;

public class CommonException extends RuntimeException{

    public CommonException(String message, Throwable cause) {
        super(message, cause);
    }

    public CommonException(Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        StackTraceElement[] stackTraceElements = this.getStackTrace();
        String messages = super.getMessage();
        return WalException.class.getCanonicalName() + (Strings.isNotNullOrEmpty(messages) ? ": " + messages : "") + "\n\tat " +
                Arrays.stream(stackTraceElements)
                        .map(StackTraceElement::toString)
                        .collect(Collectors.joining("\n\tat "));
    }

    public static String getMessage(Throwable e) {
        StackTraceElement[] stackTraceElements = e.getStackTrace();
        String messages = e.getMessage();
        return WalException.class.getCanonicalName() + (Strings.isNotNullOrEmpty(messages) ? ": " + messages : "") + "\n\tat " +
                Arrays.stream(stackTraceElements)
                        .map(StackTraceElement::toString)
                        .collect(Collectors.joining("\n\tat "));
    }
}
