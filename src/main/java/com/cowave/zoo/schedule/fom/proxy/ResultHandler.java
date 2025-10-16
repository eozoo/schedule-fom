package com.cowave.zoo.schedule.fom.proxy;

import com.cowave.zoo.schedule.fom.FomTaskResult;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public interface ResultHandler<E> {

	/**
	 * 任务结果处理
	 * @param fomTaskResult
	 * @throws Exception
	 */
	void handleResult(FomTaskResult<E> fomTaskResult) throws Exception;
}
