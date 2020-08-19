package com.mzq.usage.flink.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class ProductIncome implements Serializable {
    private String productName;
    private Integer income;
    private Integer summary;
    private Integer[] detail;

    public ProductIncome(String productName, Integer income) {
        this.productName = productName;
        this.income = income;
    }
}