package com.cowave.commons.schedule.fom;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 *
 * @param <E> 任务执行结果类型
 *
 * @author shanhm1991@163.com
 *
 */
public abstract class FomTask<E> implements Callable<FomTaskResult<E>> {

	protected volatile Logger logger = LoggerFactory.getLogger(FomTask.class);

	private static final ThreadLocal<ScheduleContext<?>> LOCAL_SCHEDULE = new ThreadLocal<>();

	protected final String id;

	// 定时线程提交时设置
	private long submitTime;

	// 任务线程启动时自己设置
	private volatile long startTime;

	// 定时线程设置，任务线程读取
	private volatile ScheduleContext<E> scheduleContext;

	// 定时线程设置，任务线程读取
	private volatile ScheduleContext.CompleteLatch<E> completeLatch;

	public FomTask(){
		this.id = this.getClass().getSimpleName();
	}

	public FomTask(String id) {
		this.id = id;
	}

	public static ScheduleContext<?> getCurrentSchedule(){
		return LOCAL_SCHEDULE.get();
	}

	@Override
	public final FomTaskResult<E> call() throws InterruptedException {
		LOCAL_SCHEDULE.set(scheduleContext);
		if(logger.isDebugEnabled() || (scheduleContext != null && scheduleContext.getScheduleConfig().logLevel() <= Level.DEBUG.toInt())){
			logger.info("task started.");
		}

		this.startTime = System.currentTimeMillis();
		final FomTaskResult<E> fomTaskResult = new FomTaskResult<>(id, submitTime, startTime);
		doCall(fomTaskResult);
		fomTaskResult.setCostTime(System.currentTimeMillis() - startTime);

		if(scheduleContext != null){
			if(completeLatch != null){
				completeLatch.addResult(fomTaskResult);
				scheduleContext.checkComplete(completeLatch);
			}
			scheduleContext.record(fomTaskResult);
		}

		if (fomTaskResult.isSuccess()) {
			if (logger.isDebugEnabled() || (scheduleContext != null && scheduleContext.getScheduleConfig().logLevel() <= Level.INFO.toInt())) {
				if (fomTaskResult.getContent() != null) {
					logger.info("{} complete {}ms {}", id, fomTaskResult.getCostTime(), fomTaskResult.getContent());
				} else {
					logger.info("{} complete {}ms", id, fomTaskResult.getCostTime());
				}
			}
		} else {
			Throwable e = null;
			if(fomTaskResult.getThrowable() != null){
				Throwable throwable = fomTaskResult.getThrowable();
				Throwable cause;
				while((cause = throwable.getCause()) != null){
					throwable = cause;
				}
				e = throwable;
			}
			if(fomTaskResult.getContent() != null){
				logger.error("{} failed {}ms {}", id, fomTaskResult.getCostTime(), fomTaskResult.getContent(), e);
			}else{
				logger.error("{} failed {}ms", id, fomTaskResult.getCostTime(), e);
			}
		}
		return fomTaskResult;
	}

	private void doCall(FomTaskResult<E> fomTaskResult){
		try {
			if(!beforeExec()){
				fomTaskResult.setSuccess(false);
				return;
			}
			fomTaskResult.setContent(exec());
		} catch(Throwable e) {
			fomTaskResult.setSuccess(false);
			fomTaskResult.setThrowable(e);
		} finally{
			try {
				afterExec(fomTaskResult.isSuccess(), fomTaskResult.getContent(), fomTaskResult.getThrowable());
			}catch(Throwable e) {
				logger.error("", e);
			}
			LOCAL_SCHEDULE.remove();
		}
	}

	public boolean beforeExec() throws Exception {
		return true;
	}

	public abstract E exec() throws Exception;

	public void afterExec(boolean isExecSuccess,  E content, Throwable e) throws Exception {

	}

	public final String getTaskId() {
		return id;
	}

	void setSubmitTime(long submitTime) {
		this.submitTime = submitTime;
	}

	public long getSubmitTime() {
		return submitTime;
	}

	public final long getStartTime() {
		return startTime;
	}

	ScheduleContext.CompleteLatch<E> getCompleteLatch() {
		return completeLatch;
	}

	void setCompleteLatch(ScheduleContext.CompleteLatch<E> completeLatch) {
		this.completeLatch = completeLatch;
	}

	ScheduleContext<E> getScheduleContext() {
		return scheduleContext;
	}

	void setScheduleContext(ScheduleContext<E> scheduleContext) {
		this.scheduleContext = scheduleContext;
		this.logger = scheduleContext.getLogger();
	}

	public String getScheduleName(){
		if(scheduleContext != null){
			return scheduleContext.getScheduleName();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public <V> V getConfig(String key){
		if(scheduleContext != null){
			return (V)scheduleContext.getScheduleConfig().get(key);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final boolean equals(Object obj) {
		if(!(obj instanceof FomTask)){
			return false;
		}
		FomTask<E> fomTask = (FomTask<E>)obj;
		return this.id.equals(fomTask.id);
	}

	@Override
	public final int hashCode() {
		return this.id.hashCode();
	}

	@Override
	public String toString() {
		return id;
	}
}
