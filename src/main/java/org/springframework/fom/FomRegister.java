package org.springframework.fom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.fom.annotation.Fom;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public class FomRegister implements BeanFactoryAware, ApplicationContextAware {

	private static final Logger logger = LoggerFactory.getLogger(FomRegister.class);

	private static DefaultListableBeanFactory defaultListableBeanFactory;

	private static ApplicationContext applicationContext;

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		defaultListableBeanFactory = (DefaultListableBeanFactory)beanFactory;
	}

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		applicationContext = context;
	}

	/**
	 * 注入外部定时器
	 * @param scheduleName   定时器beanName
	 * @param scheduleClass  被代理Class
	 * @param scheduleConfig 定时配置
	 */
	public static <T> ScheduleContext<T> register(String scheduleName, Class<?> scheduleClass, ScheduleConfig scheduleConfig) {
		Fom fom = scheduleClass.getAnnotation(Fom.class);
		if(fom == null) {
			throw new FomException(scheduleClass + " is not a fom");
		}

		String[] names = applicationContext.getBeanNamesForType(scheduleClass);
		String scheduleBeanName = names[0];

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(ScheduleContext.class);
		GenericBeanDefinition beanDefinition = (GenericBeanDefinition)builder.getBeanDefinition();
		beanDefinition.setAutowireMode(GenericBeanDefinition.AUTOWIRE_BY_TYPE);
		beanDefinition.getPropertyValues().add("scheduleClass", scheduleClass);
		beanDefinition.getPropertyValues().add("scheduleConfig", scheduleConfig);
		beanDefinition.getPropertyValues().add("scheduleName", scheduleName);
		beanDefinition.getPropertyValues().add("scheduleBeanName", scheduleBeanName);
		beanDefinition.setBeanClass(FomFactory.class);

		if (defaultListableBeanFactory.containsBeanDefinition(scheduleName)) {
			defaultListableBeanFactory.removeBeanDefinition(scheduleName);
			if (defaultListableBeanFactory.containsSingleton(scheduleName)) {
				defaultListableBeanFactory.destroySingleton(scheduleName);
			}
		}
		defaultListableBeanFactory.registerBeanDefinition(scheduleName, beanDefinition);
		ScheduleContext<T> schedule = defaultListableBeanFactory.getBean(scheduleName, ScheduleContext.class);

		ScheduleConfig config = schedule.getScheduleConfig();
		if(config.execOnLoad()){
			schedule.scheduleStart();
			logger.info("register and start schedule[{}]: {}", scheduleName, config.getConfMap());
		}else{
			logger.info("register schedule[{}]: {}", scheduleName, config.getConfMap());
		}
		return schedule;
	}
}
