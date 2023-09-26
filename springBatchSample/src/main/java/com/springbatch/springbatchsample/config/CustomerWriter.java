package com.springbatch.springbatchsample.config;

import org.springframework.batch.item.Chunk;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.springbatch.springbatchsample.entity.Customer;
import com.springbatch.springbatchsample.generic.Writer;
import com.springbatch.springbatchsample.repository.CustomerRepository;

@Component
public class CustomerWriter extends Writer<Customer> {

    @Autowired
    private CustomerRepository customerRepository;

	@Override
	public void write(Chunk<? extends Customer> chunk) throws Exception {
		System.out.println("Thread Name : -"+Thread.currentThread().getName());
        customerRepository.saveAll(chunk);
	}	
}