package com.es.es.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * jd解析对象
 * @Author: 曾睿
 * @Date: 2021/5/25 13:34
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Content{
    private String title;
    private String price;
    private String img;
}
