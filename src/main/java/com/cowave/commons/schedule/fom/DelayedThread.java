package com.cowave.commons.schedule.fom;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.DelayQueue;

import com.cowave.commons.schedule.fom.proxy.TaskCancelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * submit 提交任务的超时语义跟 定时任务有点不同，
 * <p>提交的任务会重复尝试取消，比如超时时间为五分钟，那么每隔五分钟都会尝试一次
 * <p>而定时任务只会尝试取消一次，然后一直等待任务结束
 * <p>原因在于定时的任务有定时线程可以负责等待，而手动提交的任务没有线程去帮它观察任务什么时候结束，所以暂时先这样实现
 *
 * @author shanhm1991@163.com
 *
 */
class DelayedThread extends Thread {

	private static final Logger LOGGER = LoggerFactory.getLogger(DelayedThread.class);

	private static final DelayQueue<DelayedTask> DELAYQUEUE  = new DelayQueue<>();

	@SuppressWarnings("rawtypes")
	static void detectTimeout(List<TimedFuture> futureList, boolean detectTimeoutOnEachTask){
		if(futureList.isEmpty()){
			return;
		}

		if(detectTimeoutOnEachTask){
			for(TimedFuture future : futureList){
				DELAYQUEUE.add(new DelayedSingleTask(future, future.getTimeOut()));
			}
		}else{
			DELAYQUEUE.add(new DelayedBatchTask(futureList, futureList.get(0).getTimeOut()));
		}
	}

	public DelayedThread(){
		this.setName("SubmitDemonThread");
		this.setDaemon(true);
	}

	@Override
	public void run() {
		while(true){
			DelayedTask delayedTask = null;
			try {
				delayedTask = DELAYQUEUE.take();
			} catch (InterruptedException e) {
				LOGGER.info("Thread DaemonSubmitThread interrupted and stoped");
				return;
			}

			if(delayedTask instanceof DelayedSingleTask){
				waitSingleTask((DelayedSingleTask)delayedTask);
			}else{
				waitBatchTask((DelayedBatchTask)delayedTask);
			}
		}
	}

	@SuppressWarnings("rawtypes")
	private void waitSingleTask(DelayedSingleTask delayedTask){
		TimedFuture future = delayedTask.getFuture();
		int overTime = future.getTimeOut();
		if(!future.isDone()) {
			long startTime = future.getStartTime();
			if(startTime == 0){ // startTime = 0 表示任务还没启动
				DELAYQUEUE.add(new DelayedSingleTask(future, overTime));
			}else{
				long cost = System.currentTimeMillis() - future.getStartTime();
				if(cost >= overTime){
					Task task = future.getTask();
					ScheduleContext scheduleContext = task.getScheduleContext();
					Logger logger = scheduleContext.getLogger();
					logger.warn("cancle task[{}] due to time out, cost={}ms", future.getTaskId(), cost);
					try{
						scheduleContext.handleCancel(future.getTaskId(), cost);
					}catch(Exception e){
						logger.error("", e);
					}
					cancleTask(future, cost, logger);
				}else{
					DELAYQUEUE.add(new DelayedSingleTask(future, overTime - cost));
				}
			}
		}else if(future.isEnableTaskConflict()){
			ScheduleContext.cleanCompletedFutures(future.getTaskId());
		}
	}

	@SuppressWarnings({ "rawtypes" })
	private void waitBatchTask(DelayedBatchTask delayedTask){
		List<TimedFuture> futures = delayedTask.getFutureList();

		TimedFuture f1 = futures.get(0);
		Task task = f1.getTask();
		int timeOut = f1.getTimeOut();
		ScheduleContext scheduleContext = task.getScheduleContext();
		Logger logger = scheduleContext.getLogger();

		Iterator<TimedFuture> it = futures.iterator();
		while(it.hasNext()){
			TimedFuture future = it.next();
			if(!future.isDone()) {
				long cost = 0;
				if(future.getStartTime() == 0){
					logger.warn("cancle task[{}] which has not started, cost={}ms", future.getTaskId(), cost);
				}else{
					cost = System.currentTimeMillis() - future.getStartTime();
					logger.warn("cancle task[{}] due to time out, cost={}ms", future.getTaskId(), cost);
					try{
						scheduleContext.handleCancel(future.getTaskId(), cost);
					}catch(Exception e){
						logger.error("", e);
					}
				}
				cancleTask(future, cost, logger);
			}else{
				it.remove();
				if(future.isEnableTaskConflict()){
					ScheduleContext.cleanCompletedFutures(future.getTaskId());
				}
			}
		}

		if(!futures.isEmpty()){
			DELAYQUEUE.add(new DelayedBatchTask(futures, timeOut));
		}
	}

	@SuppressWarnings("rawtypes")
	private void cancleTask(TimedFuture future, long costTime, Logger logger) {
		Task<?> task = future.getTask();
		if(costTime > 0  && TaskCancelHandler.class.isAssignableFrom(task.getClass())){
			TaskCancelHandler handler = (TaskCancelHandler)task;
			try {
				handler.handleCancel(task.getTaskId(), costTime);
			} catch (Exception e) {
				logger.error("", e);
			}
		}
		future.cancel(true);
	}
}
