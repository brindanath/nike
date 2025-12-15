package com.brindys.ETLTools.configValidator.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ValidationResult {
  private List<ValidationIssue> errors = new ArrayList<>();
  private List<ValidationIssue> warnings = new ArrayList<>();
  private List<ValidationIssue> passes = new ArrayList<>();
  private int totalIssues;
  private String summary;
}