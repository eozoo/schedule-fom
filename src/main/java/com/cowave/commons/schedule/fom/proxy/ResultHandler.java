package com.cowave.commons.schedule.fom.proxy;

import com.cowave.commons.schedule.fom.Result;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public interface ResultHandler<E> {

	/**
	 * 任务结果处理
	 * @param result
	 * @throws Exception
	 */
	void handleResult(Result<E> result) throws Exception;
}
