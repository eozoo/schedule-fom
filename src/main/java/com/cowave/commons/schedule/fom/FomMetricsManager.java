package com.cowave.commons.schedule.fom;

import io.micrometer.core.instrument.*;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author shanhm1991@163.com
 */
public class FomMetricsManager implements BeanFactoryPostProcessor {

    private static ConfigurableListableBeanFactory beanFactory;

    private static MeterRegistry registry;

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory configurableListableBeanFactory) throws BeansException {
        FomMetricsManager.beanFactory = configurableListableBeanFactory;
        String[] beanNames = beanFactory.getBeanNamesForType(MeterRegistry.class);
        if(beanNames.length > 0){
            FomMetricsManager.registry = beanFactory.getBean(beanNames[0], MeterRegistry.class);
        }
    }

    public static void scheduleTimesIncrement(String scheduleName) {
        if (registry != null) {
            registry.counter("fom.schedule.count.times", "schedule", scheduleName).increment();
        }
    }

    public static void taskSuccessIncrement(String scheduleName, String taskId) {
        if (registry != null) {
            registry.counter("fom.task.count.success", "schedule", scheduleName, "task", taskId).increment();
        }
    }

    public static void taskFailedIncrement(String scheduleName, String taskId) {
        if (registry != null) {
            registry.counter("fom.task.count.failed", "schedule", scheduleName, "task", taskId).increment();
        }
    }

    public static void taskAverageCostGauge(String scheduleName, String taskId, ScheduleStatistics statistics){
        if (registry != null) {
            registry.gauge("fom.task.cost.avg",
                    Arrays.asList(Tag.of("schedule", scheduleName), Tag.of("task", taskId)),
                    statistics, s -> statistics.getAverageCost(taskId));
        }
    }

    public static void taskMinCostGauge(String scheduleName, String taskId, AtomicLong minCost){
        if (registry != null) {
            registry.gauge("fom.task.cost.min",
                    Arrays.asList(Tag.of("schedule", scheduleName), Tag.of("task", taskId)),
                    minCost, AtomicLong::get);
        }
    }

    public static void taskMaxCostGauge(String scheduleName, String taskId, AtomicLong maxCost){
        if (registry != null) {
            registry.gauge("fom.task.cost.max",
                    Arrays.asList(Tag.of("schedule", scheduleName), Tag.of("task", taskId)),
                    maxCost, AtomicLong::get);
        }
    }

    public static void taskCostHistogram(String scheduleName, String taskId, long cost, long[] histogram) {
        if (registry != null) {
            DistributionSummary.builder("fom.task.cost.summary")
                    .tags("schedule", scheduleName, "task", taskId)
                    .sla(histogram)
                    .register(registry)
                    .record(cost);
        }
    }

    public static void taskWaitingGauge(String scheduleName, ScheduleConfig scheduleConfig){
        if (registry != null) {
            registry.gauge("fom.task.count.waiting",
                    Collections.singletonList(Tag.of("schedule", scheduleName)),
                    scheduleConfig, ScheduleConfig::getWaiting);
        }
    }

    public static void taskActiveGauge(String scheduleName, ScheduleConfig scheduleConfig){
        if (registry != null) {
            registry.gauge("fom.task.count.active",
                    Collections.singletonList(Tag.of("schedule", scheduleName)),
                    scheduleConfig, ScheduleConfig::getActives);
        }
    }
}
