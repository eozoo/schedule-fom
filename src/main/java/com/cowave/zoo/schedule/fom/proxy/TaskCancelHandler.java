package com.cowave.zoo.schedule.fom.proxy;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public interface TaskCancelHandler {

	/**
	 * 任务取消处理
	 * @param taskId 任务id
	 * @param costTime 任务耗时
	 * @throws Exception
	 */
	void handleCancel(String taskId, long costTime) throws Exception;
}
