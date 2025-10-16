package com.cowave.zoo.schedule.fom;

import static com.cowave.zoo.schedule.fom.State.INITED;
import static com.cowave.zoo.schedule.fom.State.RUNNING;
import static com.cowave.zoo.schedule.fom.State.SLEEPING;
import static com.cowave.zoo.schedule.fom.State.STOPPED;
import static com.cowave.zoo.schedule.fom.State.STOPPING;

import java.lang.reflect.Field;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import com.cowave.zoo.schedule.fom.proxy.CompleteHandler;
import com.cowave.zoo.schedule.fom.proxy.ResultHandler;
import com.cowave.zoo.schedule.fom.proxy.ScheduleFactory;
import com.cowave.zoo.schedule.fom.proxy.TaskCancelHandler;
import com.cowave.zoo.schedule.fom.proxy.TerminateHandler;
import com.cowave.zoo.schedule.fom.support.FomEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public class ScheduleContext<E> implements ScheduleFactory<E>, CompleteHandler<E>, ResultHandler<E>, TerminateHandler, TaskCancelHandler, ApplicationContextAware {

	// 所有schedule共用，以便根据id检测任务冲突
	private static final Map<String,TimedFuture<FomTaskResult<?>>> submitMap = new HashMap<>(512);

	// 当前提交的任务Future
	private final List<TimedFuture<FomTaskResult<E>>> submitFutures = new ArrayList<>();

	private final ScheduleConfig scheduleConfig = new ScheduleConfig();

	private final ScheduleStatistics scheduleStatistics = new ScheduleStatistics();

	// schedule加载时间
	private final long loadTime = System.currentTimeMillis();

	private String scheduleName;

	private String scheduleBeanName;

	private boolean external;

	// 最近一次执行时间
	private volatile long lastTime;

	// 下次执行时间
	private volatile long nextTime;

	// 执行次数
	private final AtomicLong scheduleTimes = new AtomicLong(0);

	// 是否是启动后的第一次执行
	private boolean isFirstRun = true;

	private State state = INITED;

	private ApplicationContext applicationContext;

	// running 状态纪录exec请求
	private final AtomicBoolean nextTimeHasSated = new AtomicBoolean(false);

	private volatile boolean enableTaskConflict = false;

	protected Logger logger = LoggerFactory.getLogger(ScheduleContext.class);

	/** 以下属性是为了支持 submitBatch 接口 **/

	// 记录外部线程submitBatch提交次数，与定时任务无关
	private final AtomicLong batchSubmits = new AtomicLong();

	public ScheduleContext(){

	}

	public ScheduleContext(boolean needInit){
		if(needInit){
			scheduleConfig.refresh(scheduleBeanName);
		}
	}

	void setEnableTaskConflict(boolean enableTaskConflict){
		this.enableTaskConflict = enableTaskConflict;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public ScheduleInfo getScheduleInfo(){
		return new ScheduleInfo(this);
	}

	public String getScheduleName() {
		return scheduleName;
	}

	public void setScheduleName(String scheduleName) {
		this.scheduleName = scheduleName;
	}

	public void setScheduleBeanName(String scheduleBeanName) {
		this.scheduleBeanName = scheduleBeanName;
	}

	public String getScheduleBeanName() {
		return scheduleBeanName;
	}

	public long getLoadTime() {
		return loadTime;
	}

	public long getLastTime() {
		return lastTime;
	}

	public long getNextTime() {
		return nextTime;
	}

	public long getScheduleTimes() {
		return scheduleTimes.get();
	}

	public Logger getLogger() {
		return logger;
	}

	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	public ScheduleConfig getScheduleConfig() {
		return scheduleConfig;
	}

	public ScheduleStatistics getScheduleStatistics() {
		return scheduleStatistics;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onTerminate(long execTimes, long lastExecTime) {
		// 这里要从原对象方法调用到代理对象中的方法
		ScheduleContext<E> scheduleContext;
		if(applicationContext != null && scheduleBeanName != null
				&& (scheduleContext = (ScheduleContext<E>)applicationContext.getBean(scheduleName)) != null){
			scheduleContext.onTerminate(execTimes, lastExecTime);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onComplete(long execTimes, long lastExecTime, List<FomTaskResult<E>> fomTaskResults) throws Exception {
		ScheduleContext<E> scheduleContext;
		if(applicationContext != null && scheduleBeanName != null
				&& (scheduleContext = (ScheduleContext<E>)applicationContext.getBean(scheduleName)) != null){
			scheduleContext.onComplete(execTimes, lastExecTime, fomTaskResults);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handleCancel(String taskId, long costTime) {
		ScheduleContext<E> scheduleContext;
		if(applicationContext != null && scheduleBeanName != null
				&& (scheduleContext = (ScheduleContext<E>)applicationContext.getBean(scheduleName)) != null){
			scheduleContext.handleCancel(taskId, costTime);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Collection<? extends FomTask<E>> newScheduleTasks() throws Exception {
		ScheduleContext<E> scheduleContext;
		if(applicationContext != null
				&& (scheduleContext = (ScheduleContext<E>)applicationContext.getBean(scheduleName)) != null
				&& scheduleContext.getClass() != this.getClass()){
			return scheduleContext.newScheduleTasks();
		}

		FomTask<E> fomTask = schedul();
		if(fomTask != null){
			List<FomTask<E>> list = new ArrayList<>();
			list.add(fomTask);
			return list;
		}
		return new ArrayList<>();
	}

	public FomTask<E> schedul() throws Exception {
		return null;
	}

	private synchronized void switchState(State state) {
		this.state = state;
	}

	public synchronized State getState(){
		if(state == STOPPING && scheduleConfig.getPool().isTerminated()){
			state = STOPPED;
			isFirstRun = true;
		}
		return state;
	}

	public FomEntity<Void> scheduleStart(){
		synchronized (this) {
			switch(state){
			case INITED:
			case STOPPED:
				if(scheduleConfig.getPool().isTerminated()){
					scheduleConfig.refresh(scheduleBeanName);
				}

				enableTaskConflict = scheduleConfig.enableTaskConflict(); // 启动时读取一次，init时也读取一次
				scheduleThread = new ScheduleThread();
				scheduleThread.start();
				logger.info("schedule-{} startup", scheduleName);
				return FomEntity.instance(200, "schedule-" + scheduleName + " startup.");
			case STOPPING:
				logger.warn("schedule-{} is stopping, cann't startup.", scheduleName);
				return FomEntity.instance(501, "schedule-" + scheduleName + " is stopping, cann't startup.");
			case RUNNING:
			case SLEEPING:
				logger.warn("schedule-{} was already startup.", scheduleName);
				return FomEntity.instance(501, "schedule-" + scheduleName + " was already startup.");
			default:
				return FomEntity.instance(501, "schedule state invalid.");
			}
		}
	}

	public FomEntity<Void> scheduleStop(){
		synchronized (this) {
			switch(state){
			case INITED:
				logger.warn("schedule-{} was not startup.", scheduleName);
				return FomEntity.instance(501, "schedule-" + scheduleName + " was not startup.");
			case STOPPED:
				logger.warn("schedule-{} was already stopped.", scheduleName);
				return FomEntity.instance(501, "schedule-" + scheduleName + " was already stopped.");
			case STOPPING:
				logger.warn("schedule-{} is stopping, cann't stop.", scheduleName);
				return FomEntity.instance(501, "schedule-" + scheduleName + " is already stopping.");
			case RUNNING:
			case SLEEPING:
				state = STOPPING;
				scheduleThread.interrupt(); //尽快响应
				if(scheduleConfig.getPool().isTerminated()){
					state = STOPPED;
					isFirstRun = true;
				}
				logger.info("schedule-{} will stop soon.", scheduleName);
				return FomEntity.instance(200, "schedule-" + scheduleName + " will stop soon.");
			default:
				return FomEntity.instance(501, "schedule state invalid.");
			}
		}
	}

	public FomEntity<Void> scheduleRun(){
		synchronized (this) {
			switch(state){
			case INITED:
			case STOPPED:
				logger.warn("schedule-{} was not startup, cann't execut now.", scheduleName);
				return FomEntity.instance(501, "schedule-" + scheduleName + " was not startup, cann't execut now.");
			case STOPPING:
				logger.warn("schedule-{} is stopping, cann't execut now.", scheduleName);
				return FomEntity.instance(501, "schedule-" + scheduleName + " is stopping, cann't execut now.");
			case RUNNING:
				if(scheduleConfig.ignoreExecWhenRunning()){
					logger.warn("schedule-{} is already running.", scheduleName);
					return FomEntity.instance(501, "schedule-" + scheduleName + " is already running.");
				}else{
					if(!nextTimeHasSated.compareAndSet(false, true)){
						logger.info("schedule-{} is running, and exec was already requested.", scheduleName);
						return FomEntity.instance(501, "schedule-" + scheduleName + " is running, and exec was already requested.");
					}
					nextTime = System.currentTimeMillis();
					logger.info("schedule-{} is running, and will re-exec immediately after completion.", scheduleName);
					FomEntity.instance(200, "schedule-" + scheduleName + "  is running, and will re-exec immediately after completion .");
				}
			case SLEEPING:
				scheduleThread.interrupt();
				logger.info("schedule-{} execute now.", scheduleName);
				return FomEntity.instance(200, "schedule-" + scheduleName + " execute now.");
			default:
				return FomEntity.instance(501, "schedule state invalid.");
			}
		}
	}

	private ScheduleThread scheduleThread;

	private class ScheduleThread extends Thread {

		public ScheduleThread(){
			this.setName("schedule-" + scheduleName);
		}

		@Override
		public void run() {
			while(true){
				boolean isStopping;
				synchronized (ScheduleContext.this) {
					isStopping = state == STOPPING;
				}
				if(isStopping){
					terminate();
					return;
				}

				if(scheduleConfig.deadTime() != ScheduleConfig.DEFAULT_deadTime
						&& scheduleConfig.deadTime() < System.currentTimeMillis()) {
					logger.info("schedule-{} is going to shutdown due to deadTime", scheduleName);
					terminate();
					return;
				}

				try{
					if(isFirstRun){
						isFirstRun = false;
						if(scheduleConfig.execOnLoad()) {
							runSchedul();
						}else if(scheduleConfig.initialDelay() != ScheduleConfig.DEFAULT_initialDelay) {
							sleep(scheduleConfig.initialDelay());
							runSchedul();
						}else {
							caculateNextTime(null);
						}
					}else{
						runSchedul();
					}
				}catch(InterruptedException e){
					Thread.currentThread().interrupt(); // 保留中断请求，下面处理
				}catch (Exception e){
					logger.error("", e); // 对于Exception，认为不影响定时线程
				}catch(Throwable e){
					logger.error("schedule terminated unexpectedly", e);
					terminate();
					return;
				}

				if(nextTime > 0) {
					long waitTime = nextTime - System.currentTimeMillis();
					if(!Thread.interrupted() && waitTime > 0){
						switchState(SLEEPING);
						synchronized(this) {
							try {
								wait(waitTime);
							} catch (InterruptedException e) {
								// 响应中断：结束等待，并立即重新检测state
							}
						}
					}
				}else{
					switchState(INITED); // 定时线程退出，保留线程池
					return;
				}
			}
		}

		private void caculateNextTime(CompleteLatch<E> completeLatch) {
			Date last = new Date();
			if(lastTime > 0){
				last = new Date(lastTime);
			}

			if(scheduleConfig.cronExpression() != null){
				if(!nextTimeHasSated.compareAndSet(true, false)){
					nextTime = scheduleConfig.cronExpression().getTimeAfter(last).getTime();
				}
				waitTaskCompleted(completeLatch);
			}else if(scheduleConfig.fixedRate() > 0){
				if(!nextTimeHasSated.compareAndSet(true, false)){
					nextTime = last.getTime() + scheduleConfig.fixedRate();
				}
				waitTaskCompleted(completeLatch);
			}else if(scheduleConfig.fixedDelay() > 0){
				waitTaskCompleted(completeLatch);
				if(!nextTimeHasSated.compareAndSet(true, false)){
					nextTime = System.currentTimeMillis() + scheduleConfig.fixedDelay();
				}
			}else{
				waitTaskCompleted(completeLatch);
				nextTime = 0;
			}
		}

		@SuppressWarnings("unchecked")
		private void waitTaskCompleted(CompleteLatch<E> completeLatch){
			if(completeLatch == null){
				return;
			}

			int overTime = scheduleConfig.taskOverTime();
			boolean detectTimeoutOnEachTask = scheduleConfig.detectTimeoutOnEachTask();
			try {
				if(ScheduleConfig.DEFAULT_taskOverTime == overTime){ // 默认不检测超时
					completeLatch.waitTaskCompleted();
					cleanCompletedFutures();
				}else if(!detectTimeoutOnEachTask){ // 对整体任务算超时
					if(!completeLatch.waitTaskCompleted(overTime)){
						for(TimedFuture<FomTaskResult<E>> future : submitFutures){
							if(!future.isDone()){
								long startTime = future.getStartTime();
								long cost = 0;
								if(startTime > 0){
									cost = System.currentTimeMillis() - future.getStartTime();
									logger.warn("cancle task[{}] due to time out, cost={}ms", future.getTaskId(), cost);
									try{
										handleCancel(future.getTaskId(), cost);
									}catch(Exception e){
										logger.error("", e);
									}
								}else{
									logger.warn("cancle task[{}] which has not started, cost={}ms", future.getTaskId(), cost);
								}
								cancleTask(future, cost);
							}
						}
					}
					long taskNotCompleted = completeLatch.getTaskNotCompleted();
					if(taskNotCompleted > 0){
						logger.warn("some[{}] tasks cancel fails, which may not respond to interrupts.", taskNotCompleted);
						completeLatch.waitTaskCompleted();
					}
					cleanCompletedFutures();
				}else{ // 对每个任务单独检测超时
					if(completeLatch.waitTaskCompleted(overTime)){
						cleanCompletedFutures();
					}else{
						DelayQueue<DelayedSingleTask> delayQueue  = new DelayQueue<>();
						for(TimedFuture<FomTaskResult<E>> future : submitFutures){
							waitTaskFuture(future, delayQueue, overTime);
						}

						while(!delayQueue.isEmpty()){
							DelayedSingleTask delayedTask = delayQueue.take();
							waitTaskFuture(delayedTask.getFuture(), delayQueue, overTime);
						}

						long taskNotCompleted = completeLatch.getTaskNotCompleted();
						if(taskNotCompleted > 0){
							logger.warn("some[{}] tasks may not respond to interrupts and cancel fails.", taskNotCompleted);
							completeLatch.waitTaskCompleted();
						}
						cleanCompletedFutures();
					}
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt(); // 保留中断请求，后面检测处理
			}
		}

		private void waitTaskFuture(TimedFuture<FomTaskResult<E>> future, DelayQueue<DelayedSingleTask> delayQueue, int overTime){
			if(!future.isDone()) {
				long startTime = future.getStartTime();
				if(startTime == 0){ // startTime = 0 表示任务还没启动
					delayQueue.add(new DelayedSingleTask(future, overTime));
				}else{
					long cost = System.currentTimeMillis() - future.getStartTime();
					if(cost >= overTime){
						try{
							handleCancel(future.getTaskId(), cost);
						}catch(Exception e){
							logger.error("", e);
						}

						logger.warn("cancle task[{}] due to time out, cost={}ms", future.getTaskId(), cost);
						cancleTask(future, cost);
					}else{
						delayQueue.add(new DelayedSingleTask(future, overTime - cost));
					}
				}
			}
		}

		private void runSchedul() throws Exception {

			switchState(RUNNING);

			scheduleTimes.incrementAndGet();
			FomMetricsManager.scheduleTimesIncrement(scheduleName);

			lastTime = System.currentTimeMillis();

			CompleteLatch<E> completeLatch = new CompleteLatch<>(true, scheduleTimes.get(), lastTime);
			try{
				Collection<? extends FomTask<E>> tasks = newScheduleTasks();
				if(tasks == null || tasks.isEmpty()){
					return;
				}

				// 防止返回的是不可变的Collection
				List<FomTask<E>> fomTaskList = new ArrayList<>(tasks);
				if(enableTaskConflict){
					scheduleWithConflict(fomTaskList, completeLatch);
				}else{
					scheduleWithNoConflict(fomTaskList, completeLatch);
				}
			}finally{
				completeLatch.submitCompleted();
				checkComplete(completeLatch);
				caculateNextTime(completeLatch);
			}
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		private void scheduleWithConflict(Collection<? extends FomTask<E>> tasks, CompleteLatch<E> completeLatch){
			synchronized(submitMap){
				Iterator<? extends FomTask<E>> it = tasks.iterator();
				String taskId = null;
				try{
					while(it.hasNext()){
						FomTask<E> fomTask = it.next();
						taskId = fomTask.getTaskId();
						if (isTaskAlive(taskId)) {
							logger.warn("task[{}] is still alive, create canceled.", taskId);
							continue;
						}

						fomTask.setCompleteLatch(completeLatch);
						TimedFuture future = (TimedFuture)submit(fomTask);

						it.remove();
						submitMap.put(taskId, future);
						submitFutures.add(future);
					}
				} catch (RejectedExecutionException e) {
					logger.error("task[{}] submit rejected, and ignored task={}", taskId, tasks, e);
				}
			}
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		private void scheduleWithNoConflict(Collection<? extends FomTask<E>> tasks, CompleteLatch<E> completeLatch){
			Iterator<? extends FomTask<E>> it = tasks.iterator();
			String taskId = null;
			try{
				while(it.hasNext()){
					FomTask<E> fomTask = it.next();
					taskId = fomTask.getTaskId();
					fomTask.setCompleteLatch(completeLatch);
					TimedFuture future = (TimedFuture)submit(fomTask);

					it.remove();
					submitFutures.add(future);
				}
			} catch (RejectedExecutionException e) {
				logger.error("task[{}] submit rejected, and ignored task={}", taskId, tasks, e);
			}
		}

		private void terminate() {
			scheduleConfig.getPool().shutdown();
			if(waitShutDown()){
				try{
					onTerminate(scheduleTimes.get(), lastTime);
				}catch(Exception e){
					logger.error("", e);
				}
				switchState(STOPPED);
				isFirstRun = true;
			}

			cleanCompletedFutures();
		}

		private boolean waitShutDown(){
			boolean isStopping = false;
			while(true){
				if(!isStopping){
					synchronized (ScheduleContext.this) {
						if(state == STOPPING){
							isStopping = true;

							// 不支持中断的任务，可能自定义了手动取消操作
							for(TimedFuture<FomTaskResult<E>> future : submitFutures) {
								long startTime = future.getStartTime();
								if(!future.isDone() && startTime > 0){
									long cost = System.currentTimeMillis() - future.getStartTime();
									try{
										handleCancel(future.getTaskId(), cost);
									}catch(Exception e){
										logger.error("", e);
									}

									logger.warn("cancle task[{}] due to terminate, cost={}ms", future.getTaskId(), cost);
									cancleTask(future, cost);
								}
							}
							scheduleConfig.getPool().shutdownNow();
						}
					}
				}

				try {
					if(scheduleConfig.getPool().awaitTermination(1, TimeUnit.DAYS)){
						logger.info("schedule-{} stopped.", scheduleName);
						return true;
					}else if(isStopping){
						logger.warn("schedule-{} is still stopping, though has waiting for a day.", scheduleName);
					}
				} catch (InterruptedException e) { // 忽略所有中断请求
					logger.warn("interrupt ignored when waiting task completion");
				}
			}
		}

		private void cancleTask(TimedFuture<FomTaskResult<E>> future, long costTime) {
			FomTask<?> fomTask = future.getTask();
			if(costTime > 0  && TaskCancelHandler.class.isAssignableFrom(fomTask.getClass())){
				TaskCancelHandler handler = (TaskCancelHandler) fomTask;
				try {
					handler.handleCancel(fomTask.getTaskId(), costTime);
				} catch (Exception e) {
					logger.error("", e);
				}
			}
			future.cancel(true);
		}

	}

	public Future<FomTaskResult<E>> submit(FomTask<E> fomTask) {
		fomTask.setScheduleContext(ScheduleContext.this);

		TimedFuture<FomTaskResult<E>> future =
				scheduleConfig.getPool().submit(fomTask, scheduleConfig.taskOverTime(), scheduleConfig.enableTaskConflict());
		if(fomTask.getCompleteLatch() != null){
			fomTask.getCompleteLatch().increaseTaskNotCompleted();
		}
		if(logger.isDebugEnabled() || scheduleConfig.logLevel() <= Level.DEBUG.toInt()){
			logger.info("task[{}] submitted.", fomTask.getTaskId());
		}
		return future;
	}

	/**
	 * 批量提交任务
	 * @param tasks
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public void submitBatch(Collection<? extends FomTask<E>> tasks) {
		if(CollectionUtils.isEmpty(tasks)){
			throw new IllegalArgumentException("submit tasks cannot be empty.");
		}

		CompleteLatch<E> completeLatch = new CompleteLatch<>(false, batchSubmits.incrementAndGet(), System.currentTimeMillis());
		try{
			List<TimedFuture> futureList = null;
			if(enableTaskConflict){
				futureList = submitWithConflict(tasks, completeLatch);
			}else{
				futureList = submitWithNoConflict(tasks, completeLatch);
			}

			long overTime = scheduleConfig.taskOverTime();
			if(ScheduleConfig.DEFAULT_taskOverTime != overTime){
				DelayedThread.detectTimeout(futureList, scheduleConfig.detectTimeoutOnEachTask());
			}
		}finally{
			completeLatch.submitCompleted();
			checkComplete(completeLatch);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private List<TimedFuture> submitWithConflict(Collection<? extends FomTask<E>> tasks, CompleteLatch<E> completeLatch){
		List<TimedFuture> futureList = new ArrayList<>(tasks.size());
		synchronized(submitMap){
			Iterator<? extends FomTask<E>> it = tasks.iterator();
			String taskId = null;
			try{
				while(it.hasNext()){
					FomTask<E> fomTask = it.next();
					taskId = fomTask.getTaskId();
					if (isTaskAlive(taskId)) {
						logger.warn("task[{}] is still alive, create canceled.", taskId);
						continue;
					}

					fomTask.setCompleteLatch(completeLatch);
					TimedFuture future = (TimedFuture)submit(fomTask);

					it.remove();
					submitMap.put(taskId, future);
					futureList.add(future);
				}
			} catch (RejectedExecutionException e) {
				logger.error("task[{}] submit rejected, and ignored task={}", taskId, tasks, e);
			}
		}
		return futureList;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private List<TimedFuture> submitWithNoConflict(Collection<? extends FomTask<E>> tasks, CompleteLatch<E> completeLatch){
		List<TimedFuture> futureList = new ArrayList<>(tasks.size());
		Iterator<? extends FomTask<E>> it = tasks.iterator();
		String taskId = null;
		try{
			while(it.hasNext()){
				FomTask<E> fomTask = it.next();
				taskId = fomTask.getTaskId();
				fomTask.setCompleteLatch(completeLatch);

				TimedFuture<FomTaskResult<E>> future = (TimedFuture)submit(fomTask);
				futureList.add(future);
				it.remove();
			}
		} catch (RejectedExecutionException e) {
			logger.error("task[{}] submit rejected, and ignored task={}", taskId, tasks, e);
		}
		return futureList;
	}

	private boolean isTaskAlive(String taskId){
		Future<FomTaskResult<?>> future = submitMap.get(taskId);
		return future != null && !future.isDone();
	}

	void checkComplete(CompleteLatch<E> completeLatch) {
		String s = completeLatch.isSchedul ? "schedul" : "submit";
		boolean isLastTaskComplete = completeLatch.hasTaskCompleted();
		boolean hasSubmitCompleted = completeLatch.hasSubmitCompleted();
		if(logger.isDebugEnabled() || scheduleConfig.logLevel() <= Level.DEBUG.toInt()){
			logger.info(s + "[" + completeLatch.getSchedulTimes() + "], isSubmitFinished："
				+ hasSubmitCompleted + ", taskNotCompleted：" + completeLatch.getTaskNotCompleted());
		}

		if(isLastTaskComplete && hasSubmitCompleted){
			String time = new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(completeLatch.getSchedulTime());
			if(logger.isDebugEnabled() || scheduleConfig.logLevel() <= Level.DEBUG.toInt()){
				logger.info(completeLatch.getList().size() +  " tasks of " + s
					+ "[" + completeLatch.getSchedulTimes() + "] submitted on " + time  + " completed.");
			}
			try{
				onComplete(completeLatch.getSchedulTimes(), completeLatch.getSchedulTime(), completeLatch.getList());
			}catch(Exception e){
				logger.error("", e);
			}
			completeLatch.taskCompleted();
		}
	}

	// 可能当前schedule提交的task完成后，又有其它schedule用同样taskId提交任务并正在执行，所以在真正从submitMap中删除时还要再检测一下
	// 使用submitFutures是为了在waitTaskCompleted中检测超时不需要参与submitMap的同步
	private void cleanCompletedFutures() {
		if(enableTaskConflict){
			synchronized(submitMap) {
				for(TimedFuture<FomTaskResult<E>> currentFuture : submitFutures){
					String taskId = currentFuture.getTaskId();
					TimedFuture<FomTaskResult<?>> future = submitMap.get(taskId);
					if(future != null && future.isDone()){
						submitMap.remove(taskId);
					}
				}
			}
		}
		submitFutures.clear();
	}

	static void cleanCompletedFutures(String taskId) {
		synchronized(submitMap) {
			TimedFuture<FomTaskResult<?>> future = submitMap.get(taskId);
			if(future != null && future.isDone()){
				submitMap.remove(taskId);
			}
		}
	}

	static class CompleteLatch<E> {

		private final boolean isSchedul;

		private final long schedulTimes;

		private final long schedulTime;

		private final List<FomTaskResult<E>> list = new ArrayList<>();

		// 任务是否全部提交结束
		private volatile boolean hasSubmitCompleted = false;

		// 还没有结束的任务数
		private final AtomicInteger taskNotCompleted = new AtomicInteger(1);

		// 闭锁，等待任务全部提交并执行结束
		private final CountDownLatch latch = new CountDownLatch(1);

		public void submitCompleted(){
			hasSubmitCompleted = true;
		}

		public boolean hasSubmitCompleted(){
			return hasSubmitCompleted;
		}

		public void taskCompleted(){
			latch.countDown();
		}

		public boolean waitTaskCompleted(long taskOverTime) throws InterruptedException{
			return latch.await(taskOverTime, TimeUnit.MILLISECONDS);
		}

		public void waitTaskCompleted() throws InterruptedException{
			latch.await();
		}

		public CompleteLatch(boolean isSchedul, long schedulTimes, long schedulTime){
			this.isSchedul = isSchedul;
			this.schedulTimes = schedulTimes;
			this.schedulTime = schedulTime;
		}

		public boolean isSchedul() {
			return isSchedul;
		}

		public long getSchedulTimes() {
			return schedulTimes;
		}

		public long getSchedulTime() {
			return schedulTime;
		}

		public synchronized void addResult(FomTaskResult<E> fomTaskResult){
			list.add(fomTaskResult);
		}

		public synchronized List<FomTaskResult<E>> getList() {
			return list;
		}

		public long increaseTaskNotCompleted(){
			return taskNotCompleted.incrementAndGet();
		}

		public boolean hasTaskCompleted(){
			return taskNotCompleted.decrementAndGet() == 0;
		}

		public long getTaskNotCompleted(){
			return taskNotCompleted.get();
		}
	}

	public Map<String, String> getWaitingTasks(){
		return scheduleConfig.getWaitingTasks();
	}

	public List<Map<String, String>> getActiveTasks(){
		return scheduleConfig.getActiveTasks();
	}

	public Map<String, Object> getSuccessStat(String statDay) throws ParseException {
		return new HashMap<>();
	}

	public List<Map<String, String>> getFailedStat() {
		return new ArrayList<>();
	}

	public void saveConfig(HashMap<String, Object> map, boolean valueEnvirment) throws IllegalArgumentException, IllegalAccessException{
		Set<Field> envirmentChange = scheduleConfig.saveConfig(map);
		if(!valueEnvirment || envirmentChange.isEmpty()){
			return;
		}
		valueEnvirmentField(envirmentChange);
	}

	protected void record(FomTaskResult<E> fomTaskResult){
		scheduleStatistics.record(scheduleBeanName, fomTaskResult, scheduleConfig.histogram());
		try{
			handleResult(fomTaskResult);
		}catch(Exception e){
			logger.error("", e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handleResult(FomTaskResult<E> fomTaskResult) {
		ScheduleContext<E> scheduleContext;
		if(applicationContext != null && scheduleBeanName != null
				&& (scheduleContext = (ScheduleContext<E>)applicationContext.getBean(scheduleName)) != null){
			scheduleContext.handleResult(fomTaskResult);
		}
	}

	void valueEnvirmentField(Set<Field> envirmentChange) throws NumberFormatException, IllegalArgumentException, IllegalAccessException{
		// 修改当前schedul引用对应变量属性值，不保证线程安全
		String key = "";
		String expression;
		for(Field field: envirmentChange){
			Value value = field.getAnnotation(Value.class);
			expression = value.value();

			List<String> list = FomBeanPostProcessor.getProperties(expression);
			for(String ex : list){
				int index = ex.indexOf(":");
				if(index == -1){
					index = ex.indexOf("}");
				}
				key = ex.substring(2, index);

				Object obj = scheduleConfig.get(key);
				String confValue = String.valueOf(obj);
				expression = expression.replace(ex, confValue);
			}

			ReflectionUtils.makeAccessible(field);
			Object instance = this;
			if(scheduleBeanName != null && applicationContext != null){
				instance = applicationContext.getBean(scheduleBeanName);
			}

			switch(field.getGenericType().toString()){
			case "short":
			case "class java.lang.Short":
				field.set(instance, Short.valueOf(expression)); break;
			case "int":
			case "class java.lang.Integer":
				field.set(instance, Integer.valueOf(expression)); break;
			case "long":
			case "class java.lang.Long":
				field.set(instance, Long.valueOf(expression)); break;
			case "float":
			case "class java.lang.Float":
				field.set(instance, Float.valueOf(expression)); break;
			case "double":
			case "class java.lang.Double":
				field.set(instance, Double.valueOf(expression)); break;
			case "boolean":
			case "class java.lang.Boolean":
				field.set(instance, Boolean.valueOf(expression)); break;
			case "class java.lang.String":
				field.set(instance, expression); break;
			default:
				throw new UnsupportedOperationException("value set failed：" + instance.getClass().getName() + "." + field.getName());
			}
		}
	}

	public boolean isExternal() {
		return external;
	}

	public void setExternal(boolean external) {
		this.external = external;
	}
}
