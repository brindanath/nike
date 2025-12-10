package com.brindys.deTools.visitTypeMapper.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "vocab_mappings")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class VocabMapping {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(unique = true, nullable = false)
  private String sourceVisitType;

  @Column
  private String targetVisitType;
}