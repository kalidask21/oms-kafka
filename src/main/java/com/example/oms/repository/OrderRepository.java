package com.example.oms.repository;

import com.example.oms.domain.OrderDetail;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<OrderDetail, String> {}
