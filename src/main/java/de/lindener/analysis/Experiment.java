package de.lindener.analysis;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.TreeMap;

public class Experiment {


    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public Map<LocalDateTime, Long> getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(Map<LocalDateTime, Long> memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    public Map<LocalDateTime, Double> getSystemLoad() {
        return systemLoad;
    }

    public void setSystemLoad(Map<LocalDateTime, Double> systemLoad) {
        this.systemLoad = systemLoad;
    }

    public String getSettings() {
        return settings;
    }

    public void setSettings(String settings) {
        this.settings = settings;
    }

    public String getResultPath() {
        this.resultPath = new StringBuilder().append(Constants.ANALYSIS_RESULTS_BASE).append(this.type).append('/').append(LocalDateTime.now().toString().replace(":", "-")).toString();
        return resultPath;
    }

    private LocalDateTime startTime;
    private LocalDateTime endTime;

    private long runtime;
    private Map<LocalDateTime, Long> memoryUsage = new TreeMap<>();
    private Map<LocalDateTime, Double> systemLoad = new TreeMap<>();
    private String settings;
    private String resultPath;
    private ExperimentType type;

    public void storeExperiment() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            objectMapper.writeValue(new File(resultPath + "/experiment.json"), this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(this);
        } catch (IOException e) {
            return e.toString();
        }

    }

    public void setRuntime(long netRuntime) {
        this.runtime = runtime;
    }

    public void setType(ExperimentType type) {
        this.type = type;
    }

    public long getRuntime() {
        return runtime;
    }

    public ExperimentType getType() {
        return type;
    }

}
