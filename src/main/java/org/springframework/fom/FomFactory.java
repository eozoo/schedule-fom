package org.springframework.fom;

import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public class FomFactory implements FactoryBean<ScheduleContext<?>>, ApplicationContextAware {

	private Class<?> scheduleClass;

	private String scheduleName;

	private String scheduleBeanName;

	private ScheduleConfig scheduleConfig;

	private ApplicationContext applicationContext;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;

	}

	@Override
	public ScheduleContext<?> getObject() throws Exception {
		ScheduleContext<?> scheduleContext = new ScheduleContext<>();
		scheduleContext.setLogger(LoggerFactory.getLogger(scheduleClass)); // logger
		scheduleContext.getScheduleConfig().copy(scheduleConfig); // config
		scheduleContext.setScheduleBeanName(scheduleBeanName);
		scheduleContext.setScheduleName(scheduleName);
		scheduleContext.setExternal(true);
		scheduleContext.setApplicationContext(applicationContext);

		ScheduleConfig config = scheduleContext.getScheduleConfig();
		if(scheduleBeanName != null){
			config.refresh(scheduleBeanName);
		}else{
			config.refresh(scheduleName);
		}
		return scheduleContext;
	}

	@Override
	public Class<?> getObjectType() {
		return ScheduleContext.class;
	}

	public Class<?> getScheduleClass() {
		return scheduleClass;
	}

	public void setScheduleClass(Class<?> scheduleClass) {
		this.scheduleClass = scheduleClass;
	}

	public ScheduleConfig getScheduleConfig() {
		return scheduleConfig;
	}

	public void setScheduleConfig(ScheduleConfig scheduleConfig) {
		this.scheduleConfig = scheduleConfig;
	}

	public String getScheduleBeanName() {
		return scheduleBeanName;
	}

	public void setScheduleBeanName(String scheduleBeanName) {
		this.scheduleBeanName = scheduleBeanName;
	}

	public String getScheduleName() {
		return scheduleName;
	}

	public void setScheduleName(String scheduleName) {
		this.scheduleName = scheduleName;
	}
}
