package com.cowave.zoo.schedule.fom;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author shanhm1991@163.com
 *
 */
public class ScheduleStatistics {

	// 成功数
	private final Map<String, AtomicLong> successMap = new HashMap<>();

	// 最大耗时
	private final Map<String, AtomicLong> totalCostMap = new HashMap<>();

	// 最小耗时
	private final Map<String, AtomicLong> minCostMap = new HashMap<>();

	// 总耗时
	private final Map<String, AtomicLong> maxCostMap = new HashMap<>();

	void record(String scheduleName, FomTaskResult<?> fomTaskResult, long[] histogram){
		String taskId = fomTaskResult.getTaskId();
		if(fomTaskResult.isSuccess()){
			AtomicLong success = successMap.computeIfAbsent(taskId, k -> new AtomicLong(0));
			success.incrementAndGet();
			FomMetricsManager.taskSuccessIncrement(scheduleName, taskId);

			// cost直方图
			long cost = fomTaskResult.getCostTime();
			FomMetricsManager.taskCostHistogram(scheduleName, taskId, cost, histogram);

			// 求和
			AtomicLong totalCost = totalCostMap.computeIfAbsent(taskId, id -> {
				AtomicLong initial = new AtomicLong(0);
				FomMetricsManager.taskAverageCostGauge(scheduleName, taskId, this);
				return initial;
			});
			totalCost.addAndGet(cost);

			// min
			AtomicLong minCost = minCostMap.computeIfAbsent(taskId, id -> {
				AtomicLong initial = new AtomicLong(Long.MAX_VALUE);
				FomMetricsManager.taskMinCostGauge(scheduleName, taskId, initial);
				return initial;
			});
			minCost.getAndUpdate(current -> Math.min(current, cost));

			// max
			AtomicLong maxCost = maxCostMap.computeIfAbsent(taskId, id -> {
				AtomicLong initial = new AtomicLong(Long.MIN_VALUE);
				FomMetricsManager.taskMaxCostGauge(scheduleName, taskId, initial);
				return initial;
			});
			maxCost.getAndUpdate(current -> Math.max(current, cost));
		}else{
			FomMetricsManager.taskFailedIncrement(scheduleName, taskId);
		}
	}

	public double getAverageCost(String taskId) {
		long count = successMap.get(taskId).get();
		long total = totalCostMap.get(taskId).get();
		if(count == 0){
			return 0d;
		}
		return 1.0 * total / count;
	}
}
