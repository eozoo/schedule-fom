package com.cowave.commons.schedule.fom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public class FomStarter implements SmartLifecycle, ApplicationContextAware {

	private static final Logger logger = LoggerFactory.getLogger(FomStarter.class);

	private ApplicationContext applicationContext;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void start() {
		String[] scheduleNames = applicationContext.getBeanNamesForType(ScheduleContext.class);
		for(String scheduleName : scheduleNames){
			ScheduleContext<?> schedule = (ScheduleContext)applicationContext.getBean(scheduleName);
			ScheduleConfig config = schedule.getScheduleConfig();
			if(config.execOnLoad()){
				schedule.scheduleStart();
				logger.info("load and start schedule-{}: {}", scheduleName, schedule.getScheduleConfig().getConfMap());
			}else{
				logger.info("load schedule-{}: {}", scheduleName, schedule.getScheduleConfig().getConfMap());
			}
		}
		new DelayedThread().start(); // 启动一个守护线程，检测所有手动提交的任务超时情况
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void stop() {
		String[] scheduleNames = applicationContext.getBeanNamesForType(ScheduleContext.class);
		for(String scheduleName : scheduleNames){
			ScheduleContext<?> schedule = (ScheduleContext)applicationContext.getBean(scheduleName);
			schedule.scheduleStop();
		}
	}

	@Override
	public boolean isRunning() {
		return false;
	}
}
