package com.example.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BatchApplication implements CommandLineRunner
{
	@Autowired
	JobLauncher jobLauncher;

	@Autowired
	Job ReadWrite;


	public static void main(String[] args)
	{
		SpringApplication.run(BatchApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception
	{
		JobParameters params = new JobParametersBuilder()
				.addString("JobID", String.valueOf(System.currentTimeMillis()))
				.toJobParameters();
		jobLauncher.run(ReadWrite, params);
	}
}
