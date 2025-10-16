package com.cowave.zoo.schedule.fom.proxy;

import java.util.List;

import com.cowave.zoo.schedule.fom.FomTaskResult;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public interface CompleteHandler<E> {

	/**
	 * 任务全部完成时事件处理
	 * @param times 提交或者定时执行次数
	 * @param lastTime 本次提交或者定时执行时间
	 * @param fomTaskResults 本次任务结果集
	 * @throws Exception
	 */
	public void onComplete(long times, long lastTime, List<FomTaskResult<E>> fomTaskResults) throws Exception;
}
