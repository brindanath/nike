package com.brindys.ETLTools.visitTypeMapper.model;

import jakarta.persistence.*;
import lombok.Data;

@Entity
@Table(name = "hierarchy_mappings")
@Data
public class HierarchyMapping {

  @Id
  @Column(name = "source_visit_type")
  private String sourceVisitType;

  @Column(name = "parent_hierarchy_type")
  private String parentHierarchyType;
}