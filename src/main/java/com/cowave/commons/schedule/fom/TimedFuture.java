package com.cowave.commons.schedule.fom;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 *
 * @author shanhm1991@163.com
 *
 * @param <T> 结果类型
 */
class TimedFuture<T> extends FutureTask<T> {

	private final Task<?> task;

	private final int timeOut;

	private final boolean enableTaskConflict;

	public TimedFuture(Callable<T> callable, int timeOut, boolean enableTaskConflict){
		super(callable);
		this.timeOut = timeOut;
		this.enableTaskConflict = enableTaskConflict;
		if(callable instanceof Task){
			task = ((Task<?>)callable);
		}else{
			task = null;
		}
	}

	public long getSubmitTime() {
		return task.getSubmitTime();
	}

	public long getStartTime() {
		return task.getStartTime();
	}

	public String getScheduleName(){
		return task.getScheduleName();
	}

	public String getTaskId() {
		return task.getTaskId();
	}

	public Task<?> getTask(){
		return task;
	}

	public int getTimeOut() {
		return timeOut;
	}

	public boolean isEnableTaskConflict() {
		return enableTaskConflict;
	}
}
