package com.cowave.zoo.schedule.fom;

/**
 *
 * @author shanhm1991@163.com
 *
 */
class DelayedSingleTask extends DelayedTask {

	@SuppressWarnings("rawtypes")
	private final TimedFuture future;

	@SuppressWarnings("rawtypes")
	public DelayedSingleTask(TimedFuture future, long delayTime) {
		super(delayTime);
		this.future = future;
	}

	@SuppressWarnings("rawtypes")
	public TimedFuture getFuture() {
		return future;
	}
}
