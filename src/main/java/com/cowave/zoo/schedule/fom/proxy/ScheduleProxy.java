package com.cowave.zoo.schedule.fom.proxy;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;
import com.cowave.zoo.schedule.fom.FomTaskResult;
import com.cowave.zoo.schedule.fom.ScheduleContext;
import com.cowave.zoo.schedule.fom.FomTask;
import com.cowave.zoo.schedule.fom.annotation.Schedule;
import org.springframework.util.CollectionUtils;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public class ScheduleProxy implements MethodInterceptor {

	private ScheduleContext<?> scheduleContext;

	private String beanName;

	private Object scheduleBean;

	private Class<?> scheduleBeanClass;

	public ScheduleProxy(String beanName, ScheduleContext<?> scheduleContext, Object scheduleBean){
		this.scheduleContext = scheduleContext;
		this.beanName = beanName;
		this.scheduleBean = scheduleBean;
		if(scheduleBean != null){
			this.scheduleBeanClass = scheduleBean.getClass();
		}
	}

	@Override
	public Object intercept(Object object, Method method, Object[] args, MethodProxy methodProxy) throws Throwable {
		String methodName = method.getName();
		if("onComplete".equals(methodName)){
			return onComplete(object, method, args, methodProxy);
		}else if("onTerminate".equals(methodName)){
			return onTerminate(object, method, args, methodProxy);
		}else if("handleCancel".equals(methodName)){
			return handleCancel(object, method, args, methodProxy);
		}else if("newScheduleTasks".equals(methodName)){
			return newScheduleTasks(object, method, args, methodProxy);
		}else if("handleResult".equals(methodName)){
			return handleResult(object, method, args, methodProxy);
		}else{
			return method.invoke(scheduleContext, args);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Object onComplete(Object object, Method method, Object[] args, MethodProxy methodProxy) throws Throwable{
		Class<?>[] parameterTypes = method.getParameterTypes();
		// scheduleBean为空代表是继承的ScheduleContext
		if(scheduleBean == null || parameterTypes.length != 3
				|| !long.class.isAssignableFrom(parameterTypes[0])
				|| !long.class.isAssignableFrom(parameterTypes[1])
				|| !List.class.isAssignableFrom(parameterTypes[2])){
			return method.invoke(scheduleContext, args);
		}

		// 直接断开调用
		if(!(CompleteHandler.class.isAssignableFrom(scheduleBeanClass))){
			return null;
		}

		// 调用scheduleBean的行为
		((CompleteHandler)scheduleBean).onComplete((long)args[0], (long)args[1], (List)args[2]);
		return null;
	}

	private Object onTerminate(Object object, Method method, Object[] args, MethodProxy methodProxy) throws Throwable{
		Class<?>[] parameterTypes = method.getParameterTypes();
		if(scheduleBean == null || parameterTypes.length != 2
				|| !(long.class.isAssignableFrom(parameterTypes[0]))
				|| !(long.class.isAssignableFrom(parameterTypes[1]))){
			return method.invoke(scheduleContext, args);
		}

		if(!(TerminateHandler.class.isAssignableFrom(scheduleBeanClass))){
			return null;
		}

		((TerminateHandler)scheduleBean).onTerminate((long)args[0], (long)args[1]);
		return null;
	}

	private Object handleCancel(Object object, Method method, Object[] args, MethodProxy methodProxy) throws Throwable{
		Class<?>[] parameterTypes = method.getParameterTypes();
		if(scheduleBean == null || parameterTypes.length != 2
				|| !(String.class.isAssignableFrom(parameterTypes[0]))
				|| !(long.class.isAssignableFrom(parameterTypes[1]))){
			return method.invoke(scheduleContext, args);
		}

		if(!(TaskCancelHandler.class.isAssignableFrom(scheduleBeanClass))){
			return null;
		}

		((TaskCancelHandler)scheduleBean).handleCancel((String)args[0], (long)args[1]);
		return null;
	}

	// TODO 这里应该过滤掉本身自带的方法，比如在newScheduleTasks上添加@Schedule则忽略
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Object newScheduleTasks(Object object, Method method, Object[] args, MethodProxy methodProxy) throws Throwable{
		Class<?>[] parameterTypes = method.getParameterTypes();
		if(parameterTypes.length != 0){
			return method.invoke(scheduleContext, args);
		}

		Collection<FomTask<?>> fomTasks = new ArrayList<>();
		if(scheduleBean == null){
			fomTasks.addAll((Collection<FomTask<?>>)methodProxy.invokeSuper(object, args));
			List<Method> methods = new ArrayList<>();
			for(Method m : scheduleContext.getClass().getMethods()){
				Schedule scheduled = m.getAnnotation(Schedule.class);
				if(scheduled != null){
					methods.add(m);
				}
			}

			String scheduleName = beanName;
			if(scheduleName.startsWith("$")){
				scheduleName = scheduleName.substring(1);
			}
			for(final Method m : methods){
				FomTask<Object> fomTask = new FomTask<Object>(scheduleName + "." + m.getName()) {
					@Override
					public Object exec() throws Exception {
						return m.invoke(scheduleContext);
					}
				};
				fomTasks.add(fomTask);
			}
		}else{
			if((ScheduleFactory.class.isAssignableFrom(scheduleBeanClass))){
				ScheduleFactory scheduleFactory = (ScheduleFactory)scheduleBean;
				Collection<FomTask<?>> collection =  (Collection<FomTask<?>>)scheduleFactory.newScheduleTasks();
				if(!CollectionUtils.isEmpty(collection)){
					fomTasks.addAll(collection);
				}
			}

			List<Method> methods = new ArrayList<>();
			for(Method m : scheduleBeanClass.getMethods()){
				Schedule scheduled = m.getAnnotation(Schedule.class);
				if(scheduled != null){
					methods.add(m);
				}
			}

			String scheduleName = beanName;
			if(scheduleName.startsWith("$")){
				scheduleName = scheduleName.substring(1);
			}
			for(final Method m : methods){
				FomTask<Object> fomTask = new FomTask<Object>(scheduleName + "." + m.getName()){
					@Override
					public Object exec() throws Exception {
						return m.invoke(scheduleBean);
					}
				};
				fomTasks.add(fomTask);
			}
		}
		return fomTasks;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Object handleResult(Object object, Method method, Object[] args, MethodProxy methodProxy) throws Throwable{
		Class<?>[] parameterTypes = method.getParameterTypes();
		if(scheduleBean == null || parameterTypes.length != 1
				|| !FomTaskResult.class.isAssignableFrom(parameterTypes[0])){
			return method.invoke(scheduleContext, args);
		}

		if(!(ResultHandler.class.isAssignableFrom(scheduleBeanClass))){
			return null;
		}

		((ResultHandler)scheduleBean).handleResult((FomTaskResult)args[0]);
		return null;
	}

}
