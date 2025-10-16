package com.cowave.zoo.schedule.fom;

import java.util.List;

/**
 *
 * @author shanhm1991@163.com
 *
 */
class DelayedBatchTask extends DelayedTask {

	@SuppressWarnings("rawtypes")
	private final List<TimedFuture> futureList;

	@SuppressWarnings("rawtypes")
	public DelayedBatchTask(List<TimedFuture> futureList, long delayTime) {
		super(delayTime);
		this.futureList = futureList;
	}

	@SuppressWarnings("rawtypes")
	public List<TimedFuture> getFutureList() {
		return futureList;
	}
}
