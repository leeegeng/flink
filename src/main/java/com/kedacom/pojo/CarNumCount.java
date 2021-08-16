package com.kedacom.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CarNumCount {
    private String name;
    private int totalCarNum; // 过车数量
    private Long timewindowEnd;
}
