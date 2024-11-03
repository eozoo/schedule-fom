package org.springframework.fom;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.context.ApplicationContextException;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.fom.annotation.Fom;
import org.springframework.fom.proxy.ScheduleProxy;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public class FomBeanPostProcessor implements BeanPostProcessor, BeanFactoryAware, EmbeddedValueResolverAware {

	private ConfigurableListableBeanFactory beanFactory;

	private StringValueResolver valueResolver;

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = (ConfigurableListableBeanFactory)beanFactory;
	}

	@Override
	public void setEmbeddedValueResolver(StringValueResolver stringValueResolver) {
		this.valueResolver = stringValueResolver;
	}

	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		Class<?> clazz = bean.getClass();
		if(!(ScheduleContext.class.isAssignableFrom(clazz))){
			return bean;
		}

		ScheduleContext<?> scheduleContext = (ScheduleContext<?>)bean;

		// 插一段external专门处理
		if(scheduleContext.isExternal()) {
			return processExternal(scheduleContext, beanName);
		}

		String scheduleBeanName = scheduleContext.getScheduleBeanName();
		Fom fom = scheduleContext.getClass().getAnnotation(Fom.class);

		// @Bean不处理
		if(!StringUtils.hasText(scheduleBeanName) && fom == null){
			scheduleContext.setScheduleName(beanName);
			scheduleContext.setLogger(LoggerFactory.getLogger(scheduleContext.getClass()));
			return bean;
		}

		Object scheduleBean = null;
		if(StringUtils.hasText(scheduleBeanName)){
			scheduleBean = beanFactory.getBean(scheduleBeanName);
		}

		// Logger
		fom = clazz.getAnnotation(Fom.class);
		if(fom == null){
			fom = scheduleBean.getClass().getAnnotation(Fom.class);
			scheduleContext.setLogger(LoggerFactory.getLogger(scheduleBean.getClass()));
		}else{
			scheduleContext.setLogger(LoggerFactory.getLogger(clazz));
		}

		// 加载配置
		ScheduleConfig scheduleConfig = scheduleContext.getScheduleConfig();
		if(fom != null){ // 注解
			setCron(scheduleConfig, fom, scheduleContext, scheduleBean);
			setConf(scheduleConfig, fom);
			setValue(scheduleConfig, scheduleContext, scheduleBean);
		}else{ // xml配置

		}

		// 加载缓存配置（之前修改配置后持久化的文件）
		try {
			loadCache(beanName, scheduleContext);
		} catch (Exception e) {
			throw new ApplicationContextException("", e);
		}

		// 刷新配置
		if(StringUtils.hasText(scheduleBeanName)){
			scheduleConfig.refresh(scheduleBeanName);
		}else{
			scheduleConfig.refresh(beanName);
		}

		// 初始化时提前设置一下，因为运行过程中不允许修改
		scheduleContext.setEnableTaskConflict(scheduleConfig.enableTaskConflict());

		// 创建代理 注册容器
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(clazz);
		enhancer.setCallback(new ScheduleProxy(beanName, scheduleContext, scheduleBean));
		return enhancer.create();
	}

	private Object processExternal(ScheduleContext<?> scheduleContext, String beanName) {
		beanFactory.registerSingleton("$EXTERNAL$" + beanName, scheduleContext);
		Object scheduleBean = beanFactory.getBean(scheduleContext.getScheduleBeanName());
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(ScheduleContext.class);
		enhancer.setCallback(new ScheduleProxy(beanName, scheduleContext, scheduleBean));
		return enhancer.create();
	}

	private void loadCache(String beanName, ScheduleContext<?> scheduleContext) throws Exception{
		String cacheType = valueResolver.resolveStringValue("${spring.fom.cache.type:file}");
		if("file".equalsIgnoreCase(cacheType)){
			loadFromFile(beanName, scheduleContext);
		}else if("redis".equalsIgnoreCase(cacheType)){
			loadFromRedis(beanName, scheduleContext);
		}
	}

	@SuppressWarnings("unchecked")
	private void loadFromFile(String beanName, ScheduleContext<?> scheduleContext) throws Exception{
		String cacheFilePath = valueResolver.resolveStringValue("${spring.fom.cache.file.path:cache/schedule}");
		File file = new File(cacheFilePath + File.separator + beanName + ".cache");
		if(!file.exists()){
			return;
		}

		try(FileInputStream input = new FileInputStream(file);
				ObjectInputStream ois = new ObjectInputStream(input);){
			HashMap<String, Object> map = (HashMap<String, Object>)ois.readObject();

			String cron = (String)map.remove(ScheduleConfig.KEY_cron);
			ScheduleConfig scheduleConfig = scheduleContext.getScheduleConfig();
			scheduleConfig.cron(cron);

			Map<String, Object> originalMap = scheduleConfig.getOriginalMap();
			originalMap.putAll(map);

			Collection<List<Field>> fileds = scheduleConfig.getEnvirment().values();
			Set<Field> envirmentField = new HashSet<>();
			for(List<Field> list : fileds){
				envirmentField.addAll(list);
			}
			scheduleContext.valueEnvirmentField(envirmentField);
		}
	}

	private void loadFromRedis(String beanName, ScheduleContext<?> scheduleContext) throws Exception{
		// TODO
	}

	private void setCron(ScheduleConfig scheduleConfig, Fom fom, ScheduleContext<?> scheduleContext, Object scheduleBean){
		if(fom != null){
			if(setCron(scheduleConfig, fom.cron())
					|| setFixedRate(scheduleConfig, fom.fixedRate(), fom.fixedRateString())
					|| setFixedDelay(scheduleConfig, fom.fixedDelay(), fom.fixedDelayString())){
				return;
			}
		}
	}

	private boolean setCron(ScheduleConfig scheduleConfig, String cron){
		if(StringUtils.hasText(cron)){
			cron = valueResolver.resolveStringValue(cron);
			scheduleConfig.cron(cron);
			return true;
		}
		return false;
	}

	private boolean setFixedRate(ScheduleConfig scheduleConfig, long fixedRate, String fixedRateString){
		if(StringUtils.hasText(fixedRateString)){
			fixedRateString = valueResolver.resolveStringValue(fixedRateString);
			if(scheduleConfig.fixedRate(Long.parseLong(fixedRateString))){
				return true;
			}
		}
		if(scheduleConfig.fixedRate(fixedRate)){
			return true;
		}
		return false;
	}

	private boolean setFixedDelay(ScheduleConfig scheduleConfig, long fixedDelay, String fixedDelayString){
		if(StringUtils.hasText(fixedDelayString)){
			fixedDelayString = valueResolver.resolveStringValue(fixedDelayString);
			if(scheduleConfig.fixedDelay(Long.parseLong(fixedDelayString))){
				return true;
			}
		}
		if(scheduleConfig.fixedDelay(fixedDelay)){
			return true;
		}
		return false;
	}

	private void setConf(ScheduleConfig scheduleConfig, Fom fom){

		scheduleConfig.initialDelay(fom.initialDelay());

		scheduleConfig.deadTime(fom.deadTime());

		fom.level().toInt();

		String threadCoreString = fom.threadCoreString();
		if(StringUtils.hasText(threadCoreString)){
			threadCoreString = valueResolver.resolveStringValue(threadCoreString);
			int threadCore = Integer.parseInt(threadCoreString);
			if(ScheduleConfig.DEFAULT_threadCore != threadCore){
				scheduleConfig.threadCore(threadCore);
			}
		}else if(ScheduleConfig.DEFAULT_threadCore != fom.threadCore()){
			scheduleConfig.threadCore(fom.threadCore());
		}

		String threadMaxString = fom.threadMaxString();
		if(StringUtils.hasText(threadMaxString)){
			threadMaxString = valueResolver.resolveStringValue(threadMaxString);
			int threadMax = Integer.parseInt(threadMaxString);
			if(ScheduleConfig.DEFAULT_threadCore != threadMax){
				scheduleConfig.threadMax(threadMax);
			}
		}else if(ScheduleConfig.DEFAULT_threadCore != fom.threadMax()){
			scheduleConfig.threadMax(fom.threadMax());
		}

		String queueSize = fom.queueSizeString();
		if(!StringUtils.hasText(queueSize)
				|| !scheduleConfig.queueSize(Integer.parseInt(valueResolver.resolveStringValue(queueSize)))){
			scheduleConfig.queueSize(fom.queueSize());
		}

		String threadAliveTime = fom.threadAliveTimeString();
		if(!StringUtils.hasText(threadAliveTime)
				|| !scheduleConfig.threadAliveTime(Integer.parseInt(valueResolver.resolveStringValue(threadAliveTime)))){
			scheduleConfig.threadAliveTime(fom.threadAliveTime());
		}

		scheduleConfig.remark(valueResolver.resolveStringValue(fom.remark()));

		String execOnLoad = fom.execOnLoadString();
		if(!StringUtils.hasText(execOnLoad)
				|| !scheduleConfig.execOnLoad(Boolean.parseBoolean(valueResolver.resolveStringValue(execOnLoad)))){
			scheduleConfig.execOnLoad(fom.execOnLoad());
		}

		String taskOverTime = fom.taskOverTimeString();
		if(!StringUtils.hasText(taskOverTime)
				|| !scheduleConfig.taskOverTime(Integer.parseInt(valueResolver.resolveStringValue(taskOverTime)))){
			scheduleConfig.taskOverTime(fom.taskOverTime());
		}

		String detectTimeoutOnEachTask = fom.detectTimeoutOnEachTaskString();
		if(!StringUtils.hasText(detectTimeoutOnEachTask)
				|| !scheduleConfig.detectTimeoutOnEachTask(Boolean.parseBoolean(valueResolver.resolveStringValue(detectTimeoutOnEachTask)))){
			scheduleConfig.detectTimeoutOnEachTask(fom.detectTimeoutOnEachTask());
		}

		String ignoreExecRequestWhenRunning = fom.ignoreExecWhenRunningString();
		if(!StringUtils.hasText(ignoreExecRequestWhenRunning)
				|| !scheduleConfig.ignoreExecWhenRunning(Boolean.parseBoolean(valueResolver.resolveStringValue(ignoreExecRequestWhenRunning)))){
			scheduleConfig.ignoreExecWhenRunning(fom.ignoreExecWhenRunning());
		}

		String enableTaskConflict = fom.enableTaskConflictString();
		if(!StringUtils.hasText(enableTaskConflict)
				|| !scheduleConfig.enableTaskConflict(Boolean.parseBoolean(valueResolver.resolveStringValue(enableTaskConflict)))){
			scheduleConfig.enableTaskConflict(fom.enableTaskConflict());
		}
	}

	private void setValue(ScheduleConfig scheduleConfig, ScheduleContext<?> scheduleContext, Object scheduleBean){
		Field[] fields = null;
		if(scheduleBean != null){
			fields = scheduleBean.getClass().getDeclaredFields();
		}else{
			fields = scheduleContext.getClass().getDeclaredFields();
		}

		Map<String, List<Field>> envirment = scheduleConfig.getEnvirment();
		for(Field field : fields){
			Value value = field.getAnnotation(Value.class);
			if(value != null){
				List<String> list = getProperties(value.value());
				String key = null;
				String confValue = null;
				for(String ex : list){
					confValue = valueResolver.resolveStringValue(ex);
					int index = ex.indexOf(":");
					if(index == -1){
						index = ex.indexOf("}");
					}
					key = ex.substring(2, index);

					// 一个环境变量可能赋值给多个Field，一个Field也可能被多个环境变量赋值
					List<Field> fieldList = envirment.get(key);
					if(fieldList == null){
						fieldList = new ArrayList<>();
						envirment.put(key, fieldList);
					}
					fieldList.add(field);
					scheduleConfig.set(key, confValue);
				}

				// 配置集合中的值尽量按照对应属性的类型来（如果可以的话）
				if(list.size() == 1) {
					switch(field.getGenericType().toString()){
					case "short":
					case "class java.lang.Short":
						scheduleConfig.set(key, Short.valueOf(confValue)); break;
					case "int":
					case "class java.lang.Integer":
						scheduleConfig.set(key, Integer.valueOf(confValue)); break;
					case "long":
					case "class java.lang.Long":
						scheduleConfig.set(key, Long.valueOf(confValue)); break;
					case "float":
					case "class java.lang.Float":
						scheduleConfig.set(key, Float.valueOf(confValue)); break;
					case "double":
					case "class java.lang.Double":
						scheduleConfig.set(key, Double.valueOf(confValue)); break;
					case "boolean":
					case "class java.lang.Boolean":
						scheduleConfig.set(key, Boolean.valueOf(confValue)); break;
					}
				}
			}
		}
	}

	static List<String> getProperties(String expression){
		List<String> list = new ArrayList<>();
		int begin;
		int end = 0;
		while((begin = expression.indexOf("${", end)) != -1){
			end = expression.indexOf("}", begin);
			list.add(expression.substring(begin, end + 1));
		}
		return list;
	}
}
