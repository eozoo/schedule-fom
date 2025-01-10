package com.cowave.commons.schedule.fom;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author shanhm1991@163.com
 *
 */
class TimedExecutorPool extends ThreadPoolExecutor {

	private final Map<Task<?>, Thread> threadMap = new ConcurrentHashMap<>();

	public TimedExecutorPool(int core, int max, long aliveTime, BlockingQueue<Runnable> workQueue) {
		super(core, max, aliveTime, TimeUnit.MILLISECONDS, workQueue);
	}

	@SuppressWarnings({"rawtypes"})
	@Override
	protected void beforeExecute(Thread t, Runnable r) {
		if(r instanceof TimedFuture){
			TimedFuture future = (TimedFuture)r;
			threadMap.put(future.getTask(), t);
		}
	}

	@SuppressWarnings({"rawtypes" })
	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		if(r instanceof TimedFuture){
			TimedFuture future = (TimedFuture)r;
			threadMap.remove(future.getTask());
		}
	}

	public <T> TimedFuture<T> submit(Callable<T> callable, int timeOut, boolean enableTaskConflict) {
		if (callable == null)
			throw new NullPointerException();
		TimedFuture<T> future = newTaskFor(callable, timeOut, enableTaskConflict);
		execute(future);
		return future;
	}

	@SuppressWarnings("unchecked")
	private <T> TimedFuture<T> newTaskFor(Callable<T> callable, int timeOut, boolean enableTaskConflict) {
		if(callable instanceof Task){
			Task<T> task = (Task<T>)callable;
			task.setSubmitTime(System.currentTimeMillis());
		}
		return new TimedFuture<>(callable, timeOut, enableTaskConflict);
	}

	public Map<Task<?>, Thread> getActiveThreads() {
		return threadMap;
	}
}
