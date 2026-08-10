package org.project;

import org.project.flink.LogProcessingJob;
import org.project.service.impl.EnrichLogsImpl;
import org.project.service.impl.ParseAndNormalizeLogsImpl;
import org.project.service.impl.ValidationAndDeduplicatingLogsImpl;

public class Main {
    public static void main(String[] args) throws Exception {
        LogProcessingJob job = new LogProcessingJob(
                new ParseAndNormalizeLogsImpl(),
                new ValidationAndDeduplicatingLogsImpl(),
                new EnrichLogsImpl()
        );
        System.out.println("********************************1");
        job.start();
    }
}