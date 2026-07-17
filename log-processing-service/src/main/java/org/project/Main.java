package org.project;

import org.project.flink.LogProcessingJob;

public class Main {
    public static void main(String[] args) throws Exception {
        LogProcessingJob logProcessingJob = new LogProcessingJob();
        logProcessingJob.start();
    }
}