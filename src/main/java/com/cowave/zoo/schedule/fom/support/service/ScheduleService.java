package com.cowave.zoo.schedule.fom.support.service;

import javax.validation.constraints.NotBlank;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public interface ScheduleService {

	/**
	 * 序列化当前schedule的配置
	 */
	void serializeCurrent();

	/**
	 * 序列化schedule配置
	 */
	void serialize(@NotBlank(message = "scheduleName cannot be empty.") String scheduleName);

	/**
	 * 设置当前schedule的配置
	 */
	void putCurrentConfig(String key, Object value);

	/**
	 * 设置schedule的配置
	 */
	void putConfig(@NotBlank(message = "scheduleName cannot be empty.") String scheduleName, String key, Object value);

	/**
	 * 获取当前schedule的配置
	 */
	<V> V getCurrentConfig(String key);

	/**
	 * 获取schedule的配置
	 */
	<V> V getConfig(@NotBlank(message = "scheduleName cannot be empty.") String scheduleName, String key);
}
