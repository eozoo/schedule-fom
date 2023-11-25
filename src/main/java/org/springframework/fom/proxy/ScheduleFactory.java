package org.springframework.fom.proxy;

import java.util.Collection;

import org.springframework.fom.Task;

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
