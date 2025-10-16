package com.cowave.zoo.schedule.fom.proxy;

import java.util.Collection;

import com.cowave.zoo.schedule.fom.FomTask;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public interface ScheduleFactory<E> {

	/**
	 * 创建任务
	 * @return
	 * @throws Exception
	 */
	Collection<? extends FomTask<E>> newScheduleTasks() throws Exception;
}
