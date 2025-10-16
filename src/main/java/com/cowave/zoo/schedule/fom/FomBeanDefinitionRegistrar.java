package com.cowave.zoo.schedule.fom;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContextException;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import com.cowave.zoo.schedule.fom.annotation.EnableFom;
import com.cowave.zoo.schedule.fom.annotation.Fom;
import com.cowave.zoo.schedule.fom.support.controller.FomController;
import com.cowave.zoo.schedule.fom.support.service.FomServiceImpl;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public class FomBeanDefinitionRegistrar implements ImportBeanDefinitionRegistrar{

	private static AtomicBoolean registed = new AtomicBoolean(false);

	@Override
	public void registerBeanDefinitions(AnnotationMetadata meta, BeanDefinitionRegistry registry) {
		if(!registed.compareAndSet(false, true)){
			return;
		}

		AnnotationAttributes attrs = AnnotationAttributes.fromMap(meta.getAnnotationAttributes(EnableFom.class.getName()));
		if((boolean)attrs.get("enableFomView")){
			// FomController
			RootBeanDefinition fomController = new RootBeanDefinition(FomController.class);
			registry.registerBeanDefinition("fomController", fomController);

			// FomService
			RootBeanDefinition fomService = new RootBeanDefinition(FomServiceImpl.class);
			registry.registerBeanDefinition("fomService", fomService);
		}

		RootBeanDefinition fomMetricsManager = new RootBeanDefinition(FomMetricsManager.class);
		registry.registerBeanDefinition("fomMetricsManager", fomMetricsManager);

		// FomBeanPostProcessor
		RootBeanDefinition fomBeanPostProcessor = new RootBeanDefinition(FomBeanPostProcessor.class);
		registry.registerBeanDefinition("fomBeanPostProcessor", fomBeanPostProcessor);

		// FomStarter
		RootBeanDefinition fomStarter = new RootBeanDefinition(FomStarter.class);
		registry.registerBeanDefinition("fomStarter", fomStarter);

		// FomExternalRegister
		RootBeanDefinition fomExternalRegister = new RootBeanDefinition(FomRegister.class);
		registry.registerBeanDefinition("fomExternalRegister", fomExternalRegister);

		// FomBeanDefinition
		String[] beanNames = registry.getBeanDefinitionNames();
		Class<?> clazz;
		for(String beanName : beanNames){
			BeanDefinition beanDefinition = registry.getBeanDefinition(beanName);
			String className = beanDefinition.getBeanClassName();
			if(className != null){
				try {
					clazz = Class.forName(className);
				} catch (ClassNotFoundException e) {
					throw new ApplicationContextException("", e);
				}

				Fom fom = clazz.getAnnotation(Fom.class);
				if(fom != null && fom.autoLoad()){
					parseFomSchedule(beanName, clazz, beanDefinition, fom, registry);
				}
			}
		}
	}

	public void parseFomSchedule(String beanName, Class<?> clazz, BeanDefinition beanDefinition, Fom fom, BeanDefinitionRegistry registry){
		if(ScheduleContext.class.isAssignableFrom(clazz)){
			beanDefinition.getPropertyValues().add("scheduleName", beanName);
			registry.registerAlias(beanName,  "$" + beanName);
		}else{
			RootBeanDefinition fomBeanDefinition = new RootBeanDefinition(ScheduleContext.class);
			fomBeanDefinition.getPropertyValues().add("scheduleBeanName", beanName);
			fomBeanDefinition.getPropertyValues().add("scheduleName", "$" + beanName);
			registry.registerBeanDefinition("$" + beanName, fomBeanDefinition);
		}
	}
}
