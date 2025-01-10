package com.cowave.commons.schedule.fom.proxy;

import java.util.List;

import com.cowave.commons.schedule.fom.Result;

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
	 * @param results 本次任务结果集
	 * @throws Exception
	 */
	public void onComplete(long times, long lastTime, List<Result<E>> results) throws Exception;
}
