package com.github.quintona;

/**
 * Created by svend on 27/02/14.
 */
public class RExecutionException extends RuntimeException{

    public RExecutionException(String message) {
        super(message);
    }

    public RExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public RExecutionException(Throwable cause) {
        super(cause);
    }

}
