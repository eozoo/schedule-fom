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

	private final FomTask<?> fomTask;

	private final int timeOut;

	private final boolean enableTaskConflict;

	public TimedFuture(Callable<T> callable, int timeOut, boolean enableTaskConflict){
		super(callable);
		this.timeOut = timeOut;
		this.enableTaskConflict = enableTaskConflict;
		if(callable instanceof FomTask){
			fomTask = ((FomTask<?>)callable);
		}else{
			fomTask = null;
		}
	}

	public long getSubmitTime() {
		return fomTask.getSubmitTime();
	}

	public long getStartTime() {
		return fomTask.getStartTime();
	}

	public String getScheduleName(){
		return fomTask.getScheduleName();
	}

	public String getTaskId() {
		return fomTask.getTaskId();
	}

	public FomTask<?> getTask(){
		return fomTask;
	}

	public int getTimeOut() {
		return timeOut;
	}

	public boolean isEnableTaskConflict() {
		return enableTaskConflict;
	}
}
