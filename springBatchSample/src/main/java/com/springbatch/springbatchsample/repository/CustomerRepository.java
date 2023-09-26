package com.springbatch.springbatchsample.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.springbatch.springbatchsample.entity.Customer;

public interface CustomerRepository  extends JpaRepository<Customer,Integer> {
}