package com.cowave.commons.schedule.fom;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public class ScheduleInfo {

	private final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

	private final String scheduleName;

	private final String scheduleBeanName;

	private final String state;

	private final String stateTitile;

	private final String stateImage;

	private final long scheduleTimes;

	private final String loadTime;

	private final String lastTime;

	private final String nextTime;

	private final long success;

	private final long failed;

	private final int waiting;

	private final long active;

	private final List<Conf> config = new ArrayList<>();

	private final String loggerName;

	private String loggerLevel;

	public ScheduleInfo(ScheduleContext<?> scheduleContext){
		this.scheduleName = scheduleContext.getScheduleName();
		this.scheduleBeanName = scheduleContext.getScheduleBeanName();
		this.scheduleTimes = scheduleContext.getScheduleTimes();
		this.loadTime = dateFormat.format(scheduleContext.getLoadTime());

		if(0 == scheduleContext.getLastTime()){
			this.lastTime = "";
		}else{
			this.lastTime = dateFormat.format(scheduleContext.getLastTime());
		}

		if(0 == scheduleContext.getNextTime()){
			this.nextTime = "";
		}else{
			this.nextTime = dateFormat.format(scheduleContext.getNextTime());
		}

		this.loggerName = scheduleContext.getLogger().getName();

		State state = scheduleContext.getState();
		this.state = state.name();
		this.stateTitile = state.title();
		this.stateImage = state.src();

		ScheduleStatistics scheduleStatistics = scheduleContext.getScheduleStatistics();
		this.success = scheduleStatistics.getSuccess();
		this.failed = scheduleStatistics.getFailed();

		ScheduleConfig scheduleConfig = scheduleContext.getScheduleConfig();
		this.waiting = scheduleConfig.getWaitings();
		this.active = scheduleConfig.getActives();

		Map<String, Object> configMap = new HashMap<>();
		configMap.putAll(scheduleConfig.getConfMap());

		Map<String, Object> internalConf = ScheduleConfig.getInternalConf();

		// 内部配置
		for(String key : internalConf.keySet()){
			Object defaultVal = internalConf.get(key);
			Object val = configMap.remove(key);
			if(val == null){
				config.add(new Conf(key, defaultVal, true, false, true, false));
			}else{
				if(ScheduleConfig.KEY_cron.equals(key)){
					config.add(new Conf(key, scheduleConfig.cron(), true, false, false, false));
				}else{
					config.add(new Conf(key, val, true, false, false, false));
				}
			}
		}

		// 自定义配置 已经环境变量
		for(Entry<String, Object> entry : configMap.entrySet()){
			config.add(new Conf(entry.getKey(), entry.getValue(), false, false, false, false));
		}
	}

	public static class Conf{

		private boolean internal = false;

		private boolean onlyRead = false;

		private boolean envirment = false;

		private boolean defaultValue = false;

		private final String key;

		private final Object value;

		public Conf(String key, Object value, boolean internal, boolean onlyRead, boolean defaultValue, boolean envirment){
			this.key = key;
			this.value = value;
			this.internal = internal;
			this.onlyRead = onlyRead;
			this.envirment = envirment;
			this.defaultValue = defaultValue;
		}

		public boolean isInternal() {
			return internal;
		}

		public boolean isOnlyRead() {
			return onlyRead;
		}

		public boolean isEnvirment() {
			return envirment;
		}

		public boolean isDefaultValue() {
			return defaultValue;
		}

		public String getKey() {
			return key;
		}

		public Object getValue() {
			return value;
		}
	}

	public String getScheduleName() {
		return scheduleName;
	}

	public String getScheduleBeanName() {
		return scheduleBeanName;
	}

	public String getState() {
		return state;
	}

	public String getLoadTime() {
		return loadTime;
	}

	public String getLoggerLevel() {
		return loggerLevel;
	}

	public void setLoggerLevel(String loggerLevel) {
		this.loggerLevel = loggerLevel;
	}

	public long getScheduleTimes() {
		return scheduleTimes;
	}

	public String getLastTime() {
		return lastTime;
	}

	public String getNextTime() {
		return nextTime;
	}

	public String getLoggerName() {
		return loggerName;
	}

	public long getSuccess() {
		return success;
	}

	public long getFailed() {
		return failed;
	}

	public long getWaiting() {
		return waiting;
	}

	public long getActive() {
		return active;
	}

	public String getStateTitile() {
		return stateTitile;
	}

	public String getStateImage() {
		return stateImage;
	}

	public List<Conf> getConfig() {
		return config;
	}

	@Override
	public String toString() {
		return "{scheduleName=" + scheduleName + ", scheduleBeanName=" + scheduleBeanName + ", state="
				+ state + ", scheduleTimes=" + scheduleTimes + ", loadTime=" + loadTime + ", lastTime=" + lastTime
				+ ", nextTime=" + nextTime + ", success=" + success + ", failed=" + failed + ", waiting=" + waiting
				+ ", active=" + active + ", loggerName=" + loggerName + ", loggerLevel=" + loggerLevel + "}";
	}
}
