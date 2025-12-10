package com.brindys.deTools.visitTypeMapper.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "hierarchy_mappings")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HierarchyMapping {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(unique = true, nullable = false)
  private String sourceVisitType;

  @Column
  private String parentHierarchyType;
}