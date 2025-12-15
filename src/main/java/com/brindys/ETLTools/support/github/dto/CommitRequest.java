package com.brindys.ETLTools.support.github.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CommitRequest {
  private Map<String, Object> mapping;  // Changed to Object to accept nested structure
  private String message;
  private String author;
}