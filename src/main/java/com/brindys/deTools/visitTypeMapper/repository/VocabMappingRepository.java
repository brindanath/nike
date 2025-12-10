package com.brindys.deTools.visitTypeMapper.repository;

import com.brindys.deTools.visitTypeMapper.model.VocabMapping;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface VocabMappingRepository extends JpaRepository<VocabMapping, Long> {
  Optional<VocabMapping> findBySourceVisitType(String sourceVisitType);
  void deleteBySourceVisitType(String sourceVisitType);
}