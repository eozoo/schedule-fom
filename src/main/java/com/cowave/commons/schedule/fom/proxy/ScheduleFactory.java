package com.cowave.commons.schedule.fom.proxy;

import java.util.Collection;

import com.cowave.commons.schedule.fom.Task;

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
	Collection<? extends Task<E>> newScheduleTasks() throws Exception;
}
