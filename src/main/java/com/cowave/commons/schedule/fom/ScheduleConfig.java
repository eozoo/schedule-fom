package com.cowave.commons.schedule.fom;

import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.cowave.commons.schedule.fom.quartz.CronExpression;
import org.slf4j.event.Level;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import static org.slf4j.event.Level.WARN;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public class ScheduleConfig {

	/**
	 * 启动时是否执行
	 */
	public static final String KEY_execOnLoad = "execOnLoad";

	/**
	 * 日志级别
	 */
	public static final String KEY_logLevel = "logLevel";

	/**
	 * 定时计划：cron
	 */
	public static final String KEY_cron = "cron";

	/**
	 * 定时计划：fixedRate
	 */
	public static final String KEY_fixedRate = "fixedRate";

	/**
	 * 定时计划：fixedDelay
	 */
	public static final String KEY_fixedDelay = "fixedDelay";

	/**
	 * 线程池任务队列长度
	 */
	public static final String KEY_queueSize = "queueSize";

	/**
	 * 线程池核心线程数
	 */
	public static final String KEY_threadCore = "threadCore";

	/**
	 * 线程池最大线程数
	 */
	public static final String KEY_threadMax = "threadMax";

	/**
	 * 线程池任务线程最长空闲时间
	 */
	public static final String KEY_threadAliveTime = "threadAliveTime";

	/**
	 * 任务超时时间
	 */
	public static final String KEY_taskOverTime = "taskOverTime";

	/**
	 * 是否对每个任务单独检测超时
	 */
	public static final String KEY_detectTimeoutOnEachTask = "detectTimeoutOnEachTask";

	/**
	 * 是否检测任务冲突
	 */
	public static final String KEY_enableTaskConflict = "enableTaskConflict";

	/**
	 * Running时是否忽略执行请求
	 */
	public static final String KEY_ignoreExecWhenRunning = "ignoreExecWhenRunning";

	/**
	 * 备注
	 */
	public static final String KEY_remark = "remark";

	/**
	 * 首次执行延迟时间
	 */
	public static final String KEY_initialDelay = "initialDelay";

	/**
	 * 任务截止时间
	 */
	public static final String KEY_deadTime = "deadTime";

	public static final String KET_histogram = "histogram";

	/**
	 * 加载后默认不立即执行
	 */
	public static final boolean DEFAULT_execOnLoad = true;

	/**
	 * fixedRate 默认0
	 */
	public static final int DEFAULT_fixedRate = 0;

	/**
	 * fixedDelay 默认0
	 */
	public static final int DEFAULT_fixedDelay = 0;

	/**
	 * 线程数 默认1
	 */
	public static final int DEFAULT_threadCore = 1;

	/**
	 * 线程空闲时间：默认1
	 */
	public static final int DEFAULT_threadAliveTime = 10;

	/**
	 * 任务队列长度：默认256
	 */
	public static final int DEFAULT_queueSize = 256;

	/**
	 * 任务超时时间：默认不超时
	 */
	public static final int DEFAULT_taskOverTime = 0;

	/**
	 * 默认不检测任务冲突
	 */
	public static final boolean DEFAULT_enableTaskConflict = false;

	/**
	 * 默认对每个任务单独检测超时
	 */
	public static final boolean DEFAULT_detectTimeoutOnEachTask = true;

	/**
	 * Running状态时默认忽略执行请求
	 */
	public static final boolean DEFAULT_ignoreExecWhenRunning = true;

	/**
	 * 首次执行延迟时间 默认0
	 */
	public static final long DEFAULT_initialDelay = 0;

	/**
	 * 任务截止时间 默认0
	 */
	public static final long DEFAULT_deadTime = 0;

	public static final Long[] DEFAULT_histogram = {250L, 500L, 1000L, 5000L, 10000L};

	// 内部配置，不允许直接put
	private static Map<String, Object> internalConf = new TreeMap<>();

	private final Map<String, List<Field>> envirmentConf = new HashMap<>();

	static{
		internalConf.put(KEY_logLevel, WARN.toInt());
		internalConf.put(KEY_cron, "");
		internalConf.put(KEY_fixedRate,  DEFAULT_fixedRate);
		internalConf.put(KEY_fixedDelay, DEFAULT_fixedDelay);
		internalConf.put(KEY_remark, "");
		internalConf.put(KEY_queueSize,  DEFAULT_queueSize);
		internalConf.put(KEY_threadCore, DEFAULT_threadCore);
		internalConf.put(KEY_threadMax,       DEFAULT_threadCore);
		internalConf.put(KEY_threadAliveTime, DEFAULT_threadAliveTime);
		internalConf.put(KEY_taskOverTime,    DEFAULT_taskOverTime);
		internalConf.put(KEY_execOnLoad,      DEFAULT_execOnLoad);
		internalConf.put(KEY_enableTaskConflict,           DEFAULT_enableTaskConflict);
		internalConf.put(KEY_detectTimeoutOnEachTask,      DEFAULT_detectTimeoutOnEachTask);
		internalConf.put(KEY_ignoreExecWhenRunning, DEFAULT_ignoreExecWhenRunning);
		internalConf.put(KEY_initialDelay, DEFAULT_initialDelay);
		internalConf.put(KEY_deadTime, DEFAULT_deadTime);
		internalConf.put(KET_histogram, DEFAULT_histogram);
	}

	private final ConcurrentHashMap<String, Object> confMap = new ConcurrentHashMap<>();

	private TimedExecutorPool pool;

	public void refresh(String scheduleName){
		int core = threadCore();
		int max = threadMax();
		int aliveTime = threadAliveTime();
		int queueSize = queueSize();
		pool = new TimedExecutorPool(core, max, aliveTime, new LinkedBlockingQueue<>(queueSize));
		pool.allowCoreThreadTimeOut(true);
		FomMetricsManager.taskActiveGauge(scheduleName, this);
		FomMetricsManager.taskWaitingGauge(scheduleName, this);
	}

	static Map<String, Object> getInternalConf() {
		return internalConf;
	}

	Map<String, List<Field>> getEnvirment() {
		return envirmentConf;
	}

	TimedExecutorPool getPool() {
		return pool;
	}

	public Map<String, Object> getConfMap() {
		Map<String, Object> map = new HashMap<>();
		map.putAll(confMap);
		map.put(KEY_cron, cron());
		return map;
	}

	Map<String, Object> getOriginalMap() {
		return confMap;
	}

	void copy(ScheduleConfig scheduleConfig) {
		confMap.putAll(scheduleConfig.confMap);
	}

	public boolean containsKey(String key){
		return confMap.containsKey(key);
	}

	/** info of pool  **/
	long getActives(){
		return pool == null ? 0 : pool.getActiveCount();
	}

	int getWaiting(){
		return pool == null ? 0 : pool.getQueue().size();
	}

	long getCreated(){
		return pool == null ? 0 : pool.getTaskCount();
	}

	long getCompleted(){
		return pool == null ? 0 : pool.getCompletedTaskCount();
	}

	@SuppressWarnings("unchecked")
	Map<FomTask<?>, Thread> getActiveThreads() {
		return pool == null ? new HashMap<>() : pool.getActiveThreads();
	}

	@SuppressWarnings("rawtypes")
	Map<String, Object> getWaitingDetail(){
		Map<String, Object> map = new HashMap<>();
		if(pool == null){
			return map;
		}

		Object[] array = pool.getQueue().toArray();
		if(array == null || array.length == 0){
			return map;
		}

		DateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss SSS");
		for(Object obj : array){
			if(obj instanceof TimedFuture){
				TimedFuture future = (TimedFuture)obj;
				map.put(future.getTaskId(), format.format(future.getSubmitTime()));
			}
		}
		return map;
	}

	// get/set of config  下面的set方法其实都有先获取后判断的线程安全问题，不过对于配置更新来说，这个影响可以忽略
	public boolean set(String key, Object value) {
		if(internalConf.containsKey(key)){
			throw new UnsupportedOperationException("cannot override internal config:" + key);
		}

		if(value.equals(get(key))){
			return false;
		}
		confMap.put(key, value);
		return true;
	}

	@SuppressWarnings("unchecked")
	public <V> V get(String key){
		return (V)confMap.get(key);
	}

	public String getString(String key, String defaultValue){
		return MapUtils.getString(confMap, key, defaultValue);
	}

	public int getInt(String key, int defaultValue){
		return MapUtils.getIntValue(confMap, key, defaultValue);
	}

	public long getLong(String key, long defaultValue){
		return MapUtils.getLongValue(confMap, key, defaultValue);
	}

	public boolean getBoolean(String key, boolean defaultValue){
		return MapUtils.getBooleanValue(confMap, key, defaultValue);
	}

	private String buildMsg(Object... args){
		StringBuilder builder = new StringBuilder();
		for(Object obj : args){
			builder.append(obj);
		}
		return builder.toString();
	}

	// get/set of internal config
	public int logLevel(){
		return MapUtils.getIntValue(confMap, KEY_logLevel, WARN.toInt());
	}

	public void logLevel(Level level){
		confMap.put(KEY_logLevel, level.toInt());
	}

	public CronExpression cronExpression(){
		return (CronExpression)confMap.get(KEY_cron);
	}

	public String cron(){
		CronExpression cronExpression = cronExpression();
		if(cronExpression != null){
			return cronExpression.getCronExpression();
		}
		return null;
	}

	public boolean cron(String cron){
		if(!StringUtils.hasText(cron)){
			return false;
		}

		CronExpression cronExpression;
		try {
			cronExpression = new CronExpression(cron);
		} catch (ParseException e) {
			throw new IllegalArgumentException("cronExpression cannot parsed", e);
		}

		if(!cronExpression.equals(cronExpression())){
			confMap.put(KEY_cron, cronExpression);
			return true;
		}
		return false;
	}

	public long fixedRate(){
		return MapUtils.getLongValue(confMap, KEY_fixedRate, DEFAULT_fixedRate);
	}

	public boolean fixedRate(long fixedRate){
		if(fixedRate > DEFAULT_fixedRate && fixedRate != fixedRate()){
			confMap.put(KEY_fixedRate, fixedRate);
			return true;
		}
		return false;
	}

	public long fixedDelay(){
		return MapUtils.getLongValue(confMap, KEY_fixedDelay, DEFAULT_fixedDelay);
	}

	public boolean fixedDelay(long fixedDelay){
		if(fixedDelay > DEFAULT_fixedDelay && fixedDelay != fixedDelay()){
			confMap.put(KEY_fixedDelay, fixedDelay);
			return true;
		}
		return false;
	}

	public String remark(){
		return MapUtils.getString(confMap, KEY_remark, "");
	}

	public boolean remark(String remark){
		if(remark.equals(remark())){
			return false;
		}
		confMap.put(KEY_remark, remark);
		return true;
	}

	public int threadCore(){
		return MapUtils.getIntValue(confMap, KEY_threadCore, DEFAULT_threadCore);
	}

	public boolean threadCore(int threadCore){
		Assert.isTrue(threadCore >= DEFAULT_threadCore,
				buildMsg(KEY_threadCore, " cannot be less than ", DEFAULT_threadCore));
		if(threadCore == threadCore()){
			return false;
		}

		confMap.put(KEY_threadCore, threadCore);
		int threadMax = threadMax();
		if(threadMax < threadCore){
			threadMax = threadCore;
			confMap.put(KEY_threadMax, threadMax);
		}

		if(pool != null && pool.getCorePoolSize() != threadCore){
			pool.setCorePoolSize(threadCore);
			pool.setMaximumPoolSize(threadMax);
		}
		return true;
	}

	public int threadMax(){
		return MapUtils.getIntValue(confMap, KEY_threadMax, DEFAULT_threadCore);
	}

	public boolean threadMax(int threadMax){
		Assert.isTrue(threadMax >= DEFAULT_threadCore,
				buildMsg(KEY_threadMax, " cannot be less than ", DEFAULT_threadCore));
		if(threadMax == threadMax()){
			return false;
		}

		confMap.put(KEY_threadMax, threadMax);
		int threadCore = threadCore();
		if(threadCore > threadMax){
			threadCore = threadMax;
			confMap.put(KEY_threadCore, threadCore);
		}

		if(pool != null && pool.getMaximumPoolSize() != threadMax){
			pool.setCorePoolSize(threadCore);
			pool.setMaximumPoolSize(threadMax);
		}
		return true;
	}

	public int threadAliveTime(){
		return MapUtils.getIntValue(confMap, KEY_threadAliveTime, DEFAULT_threadAliveTime);
	}

	public boolean threadAliveTime(int aliveTime){
		Assert.isTrue(aliveTime >= DEFAULT_threadAliveTime,
				buildMsg(KEY_threadAliveTime, " cannot be less than ", DEFAULT_threadAliveTime));
		if(aliveTime == threadAliveTime()){
			return false;
		}
		confMap.put(KEY_threadAliveTime, aliveTime);
		if(pool != null && pool.getKeepAliveTime(TimeUnit.MILLISECONDS) != aliveTime){
			pool.setKeepAliveTime(aliveTime, TimeUnit.MILLISECONDS);
		}
		return true;
	}

	public int taskOverTime(){
		return MapUtils.getIntValue(confMap, KEY_taskOverTime, DEFAULT_taskOverTime);
	}

	public boolean taskOverTime(int overTime){
		Assert.isTrue(overTime == DEFAULT_taskOverTime||
				overTime > 1000, buildMsg(KEY_taskOverTime, " cannot be less than 1000"));
		if(overTime == taskOverTime()){
			return false;
		}
		confMap.put(KEY_taskOverTime, overTime);
		return true;
	}

	public boolean execOnLoad(){
		return MapUtils.getBoolean(confMap, KEY_execOnLoad, DEFAULT_execOnLoad);
	}

	public boolean execOnLoad(boolean execOnLoad) {
		if(execOnLoad == execOnLoad()){
			return false;
		}
		confMap.put(KEY_execOnLoad, execOnLoad);
		return true;
	}

	public void histogram(long[] array){
		confMap.put(KET_histogram, array);
	}

	public long[] histogram(){
		return (long[])confMap.get(KET_histogram);
	}

	public boolean detectTimeoutOnEachTask(){
		return MapUtils.getBoolean(confMap, KEY_detectTimeoutOnEachTask, DEFAULT_detectTimeoutOnEachTask);
	}

	public boolean detectTimeoutOnEachTask(boolean detectTimeoutOnEachTask){
		if(detectTimeoutOnEachTask == detectTimeoutOnEachTask()){
			return false;
		}
		confMap.put(KEY_detectTimeoutOnEachTask, detectTimeoutOnEachTask);
		return true;
	}

	public boolean enableTaskConflict(){
		return MapUtils.getBoolean(confMap, KEY_enableTaskConflict, DEFAULT_enableTaskConflict);
	}

	public boolean enableTaskConflict(boolean enableTaskConflict){
		if(enableTaskConflict == enableTaskConflict()){
			return false;
		}
		confMap.put(KEY_enableTaskConflict, enableTaskConflict);
		return true;
	}

	public long initialDelay() {
		return MapUtils.getLong(confMap, KEY_initialDelay, DEFAULT_initialDelay);
	}

	public boolean initialDelay(long initialDelay) {
		if(initialDelay == initialDelay()) {
			return false;
		}
		confMap.put(KEY_initialDelay, initialDelay);
		return true;
	}

	public long deadTime() {
		return MapUtils.getLong(confMap, KEY_deadTime, DEFAULT_deadTime);
	}

	public boolean deadTime(long deadTime) {
		if(deadTime == deadTime()) {
			return false;
		}
		confMap.put(KEY_deadTime, deadTime);
		return true;
	}

	public boolean ignoreExecWhenRunning(){
		return MapUtils.getBoolean(confMap, KEY_ignoreExecWhenRunning, DEFAULT_ignoreExecWhenRunning);
	}

	public boolean ignoreExecWhenRunning(boolean ignoreExecWhenRunning){
		if(ignoreExecWhenRunning == ignoreExecWhenRunning()){
			return false;
		}
		confMap.put(KEY_ignoreExecWhenRunning, ignoreExecWhenRunning);
		return true;
	}

	public int queueSize(){
		return MapUtils.getIntValue(confMap, KEY_queueSize, DEFAULT_queueSize);
	}

	boolean queueSize(int queueSize){
		Assert.isTrue(queueSize >= 1, buildMsg(KEY_queueSize, " cannot be less than 1"));
		if(queueSize == queueSize()){
			return false;
		}
		confMap.put(KEY_queueSize, queueSize);
		return true;
	}

	@Override
	public String toString() {
		return confMap.toString();
	}

	Map<String, String> getWaitingTasks(){
		Map<String, String> map = new HashMap<>();
		if(pool == null){
			return map;
		}
		Object[] array = pool.getQueue().toArray();
		if(array.length == 0){
			return map;
		}

		DateFormat format = new SimpleDateFormat("yyyyMMdd HH:mm:ss SSS");
		for(Object obj : array){
			if(obj instanceof TimedFuture){
				TimedFuture<?> future = (TimedFuture<?>)obj;
				map.put(future.getTaskId(), format.format(future.getSubmitTime()));
			}
		}
		return map;
	}

	List<Map<String, String>> getActiveTasks(){
		List<Map<String, String>> list = new ArrayList<>();
		if(pool == null){
			return list;
		}

		DateFormat format = new SimpleDateFormat("yyyyMMdd HH:mm:ss SSS");
		for(Entry<FomTask<?>, Thread> entry : pool.getActiveThreads().entrySet()){
			FomTask<?> fomTask = entry.getKey();
			Thread thread = entry.getValue();

			Map<String, String> map = new HashMap<>();
			map.put("id", fomTask.getTaskId());
			map.put("submitTime", format.format(fomTask.getSubmitTime()));
			map.put("startTime", format.format(fomTask.getStartTime()));

			StringBuilder builder = new StringBuilder();
			for(StackTraceElement stack : thread.getStackTrace()){
				builder.append(stack).append("<br>");
			}
			map.put("stack", builder.toString());
			list.add(map);
		}
		return list;
	}

	Set<Field> saveConfig(HashMap<String, Object> map){
		Set<Field> envirmentFieldChange = new HashSet<>();
		for(Entry<String, Object> entry : map.entrySet()){
			String key = entry.getKey();
			Object value = entry.getValue();
			if(internalConf.containsKey(key)){
				saveInternalConfig(key, value);
			}else{
				List<Field> list = envirmentConf.get(key);
				if(list != null){
					envirmentFieldChange.addAll(list);
				}

				// 保存配置的时候，尽量按照不改变原配置值的类型
				Object oldValue = confMap.get(key);
				if(oldValue != null){
					Class<?> clazz = oldValue.getClass();
					if(Integer.class == clazz){
						confMap.put(key, Integer.valueOf(value.toString()));
					}else if(Long.class == clazz){
						confMap.put(key, Long.valueOf(value.toString()));
					}else if(Float.class == clazz){
						confMap.put(key, Float.valueOf(value.toString()));
					}else if(Double.class == clazz){
						confMap.put(key, Double.valueOf(value.toString()));
					}else if(Boolean.class == clazz){
						confMap.put(key, Boolean.valueOf(value.toString()));
					}else if(Short.class == clazz){
						confMap.put(key, Short.valueOf(value.toString()));
					}else{
						confMap.put(key, value);
					}
				}else{
					confMap.put(key, value);
				}
			}
		}
		return envirmentFieldChange;
	}

	// 配置有限，这里简单处理下，这样做主要是为了自我保护，防止配置被恶意修改
	private void saveInternalConfig(String key, Object value){
		switch(key){
		case KEY_cron:
			cron(value.toString()); return;
		case KEY_fixedRate:
			fixedRate(Long.valueOf(value.toString())); return;
		case KEY_fixedDelay:
			fixedDelay(Long.valueOf(value.toString())); return;
		case KEY_remark:
			remark(value.toString()); return;
		case KEY_threadCore:
			threadCore(Integer.valueOf(value.toString())); return;
		case KEY_threadMax:
			threadMax(Integer.valueOf(value.toString())); return;
		case KEY_threadAliveTime:
			threadAliveTime(Integer.valueOf(value.toString())); return;
		case KEY_taskOverTime:
			taskOverTime(Integer.valueOf(value.toString())); return;
		case KEY_detectTimeoutOnEachTask:
			detectTimeoutOnEachTask(Boolean.valueOf(value.toString())); return;
		case KEY_ignoreExecWhenRunning:
			ignoreExecWhenRunning(Boolean.valueOf(value.toString())); return;
		case KEY_enableTaskConflict:
			enableTaskConflict(Boolean.valueOf(value.toString())); return;
		case KEY_queueSize:
			queueSize(Integer.valueOf(value.toString())); return;
		case KEY_execOnLoad:
			execOnLoad(Boolean.valueOf(value.toString())); return;
		case KET_histogram:
			histogram((long[])value); return;
		default:
			throw new UnsupportedOperationException("config[" + key + "] cannot be change");
		}
	}
}
