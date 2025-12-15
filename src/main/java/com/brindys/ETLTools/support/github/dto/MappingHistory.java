package com.brindys.ETLTools.support.github.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MappingHistory {
  private String filename;
  private String commitMessage;
  private String author;
  private LocalDateTime date;
  private String downloadUrl;
}