package org.springframework.fom.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.fom.ScheduleConfig;
import org.springframework.stereotype.Component;

/**
 *
 * @author shanhm1991@163.com
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface Fom {

	@AliasFor(annotation = Component.class)
	String value() default "";

	/**
	 * 是否自动加载
	 */
	boolean autoLoad() default true;

	/**
	 * 加载后是否立即执行
	 */
	boolean execOnLoad() default ScheduleConfig.DEFAULT_execOnLoad;

	/**
	 * 加载后是否立即执行
	 */
	String execOnLoadString() default "";

	/**
	 * 计划表
	 */
	String cron() default "";

	/**
	 * 距上一次任务开始时的时间[ms]
	 */
	long fixedRate() default ScheduleConfig.DEFAULT_fixedRate;

	/**
	 * 距上一次任务开始时的时间[ms]
	 */
	String fixedRateString() default "";

	/**
	 * 距上一次任务结束时的时间[ms]
	 */
	long fixedDelay() default ScheduleConfig.DEFAULT_fixedDelay;

	/**
	 * 距上一次任务结束时的时间[ms]
	 */
	String fixedDelayString() default "";

	/**
	 * 核心线程数
	 */
	int threadCore() default ScheduleConfig.DEFAULT_threadCore;

	/**
	 * 核心线程数
	 */
	String threadCoreString() default "";

	/**
	 * 最大线程数
	 */
	int threadMax() default ScheduleConfig.DEFAULT_threadCore;

	/**
	 * 最大线程数
	 */
	String threadMaxString() default "";

	/**
	 * 线程最大空闲时间[ms]
	 */
	int threadAliveTime() default ScheduleConfig.DEFAULT_threadAliveTime;

	/**
	 * 线程最大空闲时间[ms]
	 */
	String threadAliveTimeString() default "";

	/**
	 * 队列长度
	 */
	int queueSize() default ScheduleConfig.DEFAULT_queueSize;

	/**
	 * 队列长度
	 */
	String queueSizeString() default "";

	/**
	 * 任务超时时间[ms]
	 */
	int taskOverTime() default ScheduleConfig.DEFAULT_taskOverTime;

	/**
	 * 任务超时时间[ms]
	 */
	String taskOverTimeString() default "";

	/**
	 * 是否对每个任务单独检测超时
	 */
	boolean detectTimeoutOnEachTask() default ScheduleConfig.DEFAULT_detectTimeoutOnEachTask;

	/**
	 * 是否对每个任务单独检测超时
	 */
	String detectTimeoutOnEachTaskString() default "";

	/**
	 * 是否检测任务冲突
	 */
	boolean enableTaskConflict() default ScheduleConfig.DEFAULT_enableTaskConflict;

	/**
	 * 是否检测任务冲突
	 */
	String enableTaskConflictString() default "";

	/**
	 * Running状态时是否忽略执行请求
	 */
	boolean ignoreExecWhenRunning() default ScheduleConfig.DEFAULT_ignoreExecWhenRunning;

	/**
	 * Running状态时是否忽略执行请求
	 */
	String ignoreExecWhenRunningString() default "";

	/**
	 * 备注
	 */
	String remark() default "";

	/**
	 * 首次执行延迟时间[ms]
	 */
	long initialDelay() default ScheduleConfig.DEFAULT_initialDelay;

	/**
	 * 任务截止时间[ms]
	 */
	long deadTime() default ScheduleConfig.DEFAULT_deadTime;
}
