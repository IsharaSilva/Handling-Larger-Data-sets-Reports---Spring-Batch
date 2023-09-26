package com.springbatch.springbatchsample.config;

import com.springbatch.springbatchsample.entity.Customer;
import com.springbatch.springbatchsample.generic.Processor;

public class CustomerProcessor extends Processor<Customer, Customer> {

    @Override
    public Customer process(Customer customer) throws Exception {
            return customer;
    }
}