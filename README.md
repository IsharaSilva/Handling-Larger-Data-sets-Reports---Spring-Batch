# Handling-larger-Data-sets-Reports---Spring-Batch

This is a simple step by step guide to handle larger data sets with reporting using batch processing.  Different types of reporting formats like CSV, XML, and PDF using batch processing are discussed here with Spring Boot and MySQL. 

Mainly focus on Spring Boot greater than version of 3.0. Starting with spring-boot 3.0, @EnableBatchProcessing annotation is discouraged. We declare manually JobRepository, and JobLauncher beans. In addition, JobBuilderFactory and StepBuilderFactory are deprecated, and it’s recommended to use JobBuilder and StepBuilder class with the name of the job/step builder [2]. 
