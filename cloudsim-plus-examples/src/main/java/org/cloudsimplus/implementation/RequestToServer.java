package org.cloudsimplus.implementation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RequestToServer {
    private String day1;
    private String day2;
    private String day3;
}
