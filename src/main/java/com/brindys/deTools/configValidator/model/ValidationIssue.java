package com.brindys.deTools.configValidator.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ValidationIssue {
  private String severity; // ERROR, WARNING, PASS
  private String category; // SYNTAX, LOGIC, BEST_PRACTICE, etc.
  private int lineNumber;
  private String message;
  private String suggestion;
  private String snippet; // Code snippet showing the issue
}