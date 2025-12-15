package com.brindys.ETLTools.visitTypeMapper.model;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "vocab_mappings")
@Data
public class VocabMapping {

  @Id
  @Column(name = "source_visit_type")
  private String sourceVisitType;

  @Column(name = "target_visit_type")
  private String targetVisitType;
}