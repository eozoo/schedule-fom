package com.cowave.zoo.schedule.fom.support.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import com.cowave.zoo.schedule.fom.ScheduleInfo;
import org.springframework.beans.factory.annotation.Autowired;
import com.cowave.zoo.schedule.fom.support.FomEntity;
import com.cowave.zoo.schedule.fom.support.FomEntity.Page;
import com.cowave.zoo.schedule.fom.support.service.FomService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * @author shanhm1991@163.com
 *
 */
@RequestMapping("/fom")
public class FomController {

	@Autowired
	private FomService fomService;

	@GetMapping
	public String index() {
		return "fom.html";
	}

	@RequestMapping("/schedule/list")
	@ResponseBody
	public FomEntity<Page<ScheduleInfo>> list() {
		List<ScheduleInfo> list = fomService.list();
		return FomEntity.success(new Page<>(list, list.size()));
	}

	@RequestMapping("/schedule/info")
	@ResponseBody
	public FomEntity<ScheduleInfo> info(String scheduleName) {
		return FomEntity.success(fomService.info(scheduleName));
	}

	@RequestMapping("/schedule/logger/level")
	@ResponseBody
	public FomEntity<String> loggerLevel(String scheduleName) {
		return FomEntity.success(fomService.getLoggerLevel(scheduleName));
	}

	@RequestMapping("/schedule/logger/level/update")
	@ResponseBody
	public FomEntity<Void> updateloggerLevel(String scheduleName, String levelName) {
		fomService.updateloggerLevel(scheduleName, levelName);
		return FomEntity.success();
	}

	@RequestMapping("/schedule/start")
	@ResponseBody
	public FomEntity<Void> start(String scheduleName) {
		return fomService.start(scheduleName);
	}

	@RequestMapping("/schedule/shutdown")
	@ResponseBody
	public FomEntity<Void> shutdown(String scheduleName) {
		return fomService.shutdown(scheduleName);
	}

	@RequestMapping("/schedule/exec")
	@ResponseBody
	public FomEntity<Void> exec(String scheduleName) {
		return fomService.exec(scheduleName);
	}

	@RequestMapping("/schedule/waitings")
	@ResponseBody
	public FomEntity<Map<String, String>> waitingTasks(String scheduleName) {
		return FomEntity.success(fomService.getWaitingTasks(scheduleName));
	}

	@RequestMapping("/schedule/actives")
	@ResponseBody
	public FomEntity<List<Map<String, String>>> activeTasks(String scheduleName) {
		return FomEntity.success(fomService.getActiveTasks(scheduleName));
	}

	@RequestMapping("/schedule/export")
	public void statExport(String scheduleName, HttpServletResponse resp) throws IOException {
		String stat = fomService.buildExport(scheduleName);
		resp.reset();
		resp.setContentType("application/octet-stream;charset=UTF-8");
		resp.addHeader("Content-Disposition", "attachment;filename=\"" + scheduleName + "." + System.currentTimeMillis() +".txt\"");
		try(PrintWriter write = resp.getWriter()){
			write.write(stat);
			write.flush();
		}
	}

	@RequestMapping("/schedule/faileds")
	@ResponseBody
	public FomEntity<List<Map<String, String>>> failedStat(String scheduleName) {
		return FomEntity.success(fomService.getFailedStat(scheduleName));
	}

	@RequestMapping("/schedule/success")
	@ResponseBody
	public FomEntity<Map<String, Object>> successStat(String scheduleName, String statDay) throws ParseException {
		return FomEntity.success(fomService.getSuccessStat(scheduleName, statDay));
	}

	@RequestMapping("/schedule/saveStatConf")
	@ResponseBody
	public FomEntity<Map<String, Object>> saveStatConf(
			String scheduleName, String statDay, String statLevel, int saveDay) throws ParseException {
		return FomEntity.success(fomService.saveStatConf(scheduleName, statDay, statLevel, saveDay));
	}

	@SuppressWarnings("unchecked")
	@RequestMapping("/schedule/config/save")
	@ResponseBody
	public FomEntity<Void> saveConfig(String scheduleName, String data) throws Exception {
		HashMap<String, Object> configMap = (HashMap<String, Object>) new ObjectMapper().readValue(data, HashMap.class);
		fomService.saveConfig(scheduleName, configMap);
		return FomEntity.success();
	}
}
