package com.springbatch.springbatchsample.config;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.springbatch.springbatchsample.entity.Customer;
import com.springbatch.springbatchsample.generic.FlatFileReader;
import com.springbatch.springbatchsample.partition.ColumnRangePartitioner;
import com.springbatch.springbatchsample.repository.CustomerRepository;

import lombok.AllArgsConstructor;

@Configuration
@AllArgsConstructor
public class CustomerJobConfig {

	private CustomerWriter customerWriter;

	@Autowired
	private CustomerRepository customerRepository;

	@Autowired
	private DataSource dataSource;

	private static final String ID = "id";
	private static final String FIRST_NAME = "firstName";
	private static final String LAST_NAME = "lastName";
	private static final String CONTACT_NO = "contactNo";
	private static final String COUNTRY = "country";
	private static final String EMAIL = "email";
	private static final String DOB = "dob";
	private static final String GENDER = "gender";
	private static final String IMPORT_PATH = "src/main/resources/customers.csv";
	private static final String OUTPUT_PATH = "src/main/resources/output/";

	@Bean
	public ItemReader<Customer> reader() {
		LineMapper<Customer> lineMapper = createLineMapper(Customer.class);
        FlatFileReader<Customer> genericReader = new FlatFileReader<>();
        return genericReader.createFlatFileReader(new FileSystemResource(IMPORT_PATH), Customer.class, lineMapper);
	}

	private LineMapper<Customer> createLineMapper(Class<Customer> targetType) {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();
        
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames(new String[]{ID, FIRST_NAME, LAST_NAME, EMAIL, GENDER, CONTACT_NO, COUNTRY, DOB});
        
        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(targetType);
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        
        return lineMapper;
    }

	@Bean
	public JdbcCursorItemReader<Customer> dbDataReader() {
		JdbcCursorItemReader<Customer> cursorItemReader = new JdbcCursorItemReader<>();
		cursorItemReader.setDataSource(dataSource);
		cursorItemReader.setSql("SELECT * FROM customers_info");
		cursorItemReader.setRowMapper(new CustomerXMLRowMapper());
		return cursorItemReader;
	}

	@Bean
	public CustomerProcessor processor() {
		return new CustomerProcessor();
	}

	@Bean
	public ColumnRangePartitioner partitioner() {
		return new ColumnRangePartitioner();
	}

	@Bean
	public PartitionHandler partitionHandler(JobRepository jobRepository,
			PlatformTransactionManager transactionManager) {
		TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();
		taskExecutorPartitionHandler.setGridSize(4);
		taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
		taskExecutorPartitionHandler.setStep(slaveStep(jobRepository, transactionManager));

		return taskExecutorPartitionHandler;
	}

	@Bean
	public FlatFileItemWriter<Customer> csvFileItemWriter() {
		FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<>();
		writer.setResource(new FileSystemResource(OUTPUT_PATH + "customers_output.csv"));

		LineAggregator<Customer> lineAggregator = createCustomerLineAggregator();
		writer.setLineAggregator(lineAggregator);

		return writer;
	}

	@Bean
	public LineAggregator<Customer> createCustomerLineAggregator() {
		DelimitedLineAggregator<Customer> lineAggregator = new DelimitedLineAggregator<>();
		lineAggregator.setDelimiter(",");

		BeanWrapperFieldExtractor<Customer> fieldExtractor = new BeanWrapperFieldExtractor<>();
		fieldExtractor.setNames(new String[] { ID, FIRST_NAME, LAST_NAME, EMAIL, GENDER, CONTACT_NO, COUNTRY, DOB });
		lineAggregator.setFieldExtractor(fieldExtractor);

		return lineAggregator;
	}

	@Bean
	public StaxEventItemWriter<Customer> xmlWriter() {
		StaxEventItemWriter<Customer> writer = new StaxEventItemWriter<>();
	    writer.setResource(null); 
	    writer.setRootTagName("customers");

	    Map<String, String> aliasesMap = new HashMap<>();
	    aliasesMap.put("Customer", "com.springbatch.springbatchsample.entity.Customer");
	    XStreamMarshaller marshaller = new XStreamMarshaller();
	    marshaller.setAliases(aliasesMap);
	    writer.setMarshaller(marshaller);
	    writer.setOverwriteOutput(true);

	    return writer;
	}
	@Bean
	public MultiResourceItemWriter<Customer> multiXmlWriter() {
	    MultiResourceItemWriter<Customer> multiWriter = new MultiResourceItemWriter<>();
	    multiWriter.setResource(new FileSystemResource(OUTPUT_PATH + "customer_output.xml"));
	    multiWriter.setItemCountLimitPerResource(250);
	    multiWriter.setDelegate(xmlWriter());
	    return multiWriter;
	}

	@Bean
	public ItemWriter<Customer> pdfItemWriter() {
		return new PdfItemWriterImpl(OUTPUT_PATH + "customers.pdf");
	}

	@Bean
	public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
	    return new StepBuilder("xml-step", jobRepository).<Customer, Customer>chunk(250, transactionManager)
	            .reader(dbDataReader()).processor(processor()).writer(multiXmlWriter()).build();
	}

	@Bean
	public Step step2(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		return new StepBuilder("csv-step", jobRepository).<Customer, Customer>chunk(250, transactionManager)
				.reader(dbDataReader()).processor(processor()).writer(csvFileItemWriter()).build();
	}

	@Bean
	public Step step3(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		return new StepBuilder("pdf-step", jobRepository).<Customer, Customer>chunk(250, transactionManager)
				.reader(dbDataReader()).processor(processor()).writer(pdfItemWriter()).build();
	}

	@Bean
	public Step slaveStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		return new StepBuilder("slaveStep", jobRepository).<Customer, Customer>chunk(250, transactionManager)
				.reader(reader()).processor(processor()).writer(customerWriter).taskExecutor(taskExecutor()).build();
	}

	@Bean
	public Step masterStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		return new StepBuilder("masterSTep", jobRepository)
				.partitioner(slaveStep(jobRepository, transactionManager).getName(), partitioner())
				.partitionHandler(partitionHandler(jobRepository, transactionManager)).build();
	}

	@Bean
	public Job runJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		Flow flow1 = new FlowBuilder<Flow>("flow1").start(step1(jobRepository, transactionManager)).build();
		Flow flow2 = new FlowBuilder<Flow>("flow2").start(slaveStep(jobRepository, transactionManager)).build();
		Flow flow3 = new FlowBuilder<Flow>("flow3").start(step2(jobRepository, transactionManager)).build();
		Flow flow4 = new FlowBuilder<Flow>("flow4").start(step3(jobRepository, transactionManager)).build();
		return new JobBuilder("importCustomers", jobRepository).start(flow1).next(flow2).next(flow3).next(flow4).end()
				.build();
	}

	@Bean
	public TaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(4);
		taskExecutor.setCorePoolSize(4);
		taskExecutor.setQueueCapacity(4);
		return taskExecutor;
	}
}