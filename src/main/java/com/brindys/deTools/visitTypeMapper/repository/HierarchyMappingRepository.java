package com.brindys.deTools.visitTypeMapper.repository;


import com.brindys.deTools.visitTypeMapper.model.HierarchyMapping;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface HierarchyMappingRepository extends JpaRepository<HierarchyMapping, Long> {
  Optional<HierarchyMapping> findBySourceVisitType(String sourceVisitType);
  void deleteBySourceVisitType(String sourceVisitType);
}