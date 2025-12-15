package com.brindys.ETLTools.support.github.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CommitResponse {
  private boolean success;
  private String commitSha;
  private String commitUrl;
  private String message;
  private LocalDateTime timestamp;

  // Convenience constructor for simple responses
  public CommitResponse(boolean success, String message) {
    this.success = success;
    this.message = message;
    this.timestamp = LocalDateTime.now();
  }
}