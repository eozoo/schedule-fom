package com.cowave.zoo.schedule.fom;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public class FomException extends RuntimeException{

	private static final long serialVersionUID = 1L;

	public FomException(String message) {
        super(message);
    }

	public FomException(String message, Throwable cause) {
        super(message, cause);
    }

}
