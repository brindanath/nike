package com.brindys.deTools.visitTypeMapper.controller;


import com.brindys.deTools.visitTypeMapper.model.HierarchyMapping;
import com.brindys.deTools.visitTypeMapper.model.VocabMapping;
import com.brindys.deTools.visitTypeMapper.repository.HierarchyMappingRepository;
import com.brindys.deTools.visitTypeMapper.repository.VocabMappingRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class MappingController {

  @Autowired
  private VocabMappingRepository vocabRepo;

  @Autowired
  private HierarchyMappingRepository hierarchyRepo;

  // ========== VOCAB MAPPINGS ==========

  @GetMapping("/vocab/all")
  public ResponseEntity<Map<String, String>> getAllVocabMappings() {
    List<VocabMapping> mappings = vocabRepo.findAll();
    Map<String, String> result = new HashMap<>();
    for (VocabMapping m : mappings) {
      result.put(m.getSourceVisitType(), m.getTargetVisitType());
    }
    return ResponseEntity.ok(result);
  }

  @PostMapping("/vocab/save")
  @Transactional
  public ResponseEntity<String> saveVocabMapping(@RequestBody Map<String, String> payload) {
    String source = payload.get("source");
    String target = payload.get("target");

    if (source == null || source.trim().isEmpty()) {
      return ResponseEntity.badRequest().body("Source visit type required");
    }

    VocabMapping mapping = vocabRepo.findBySourceVisitType(source)
        .orElse(new VocabMapping());
    mapping.setSourceVisitType(source);
    mapping.setTargetVisitType(target != null ? target : "");
    vocabRepo.save(mapping);

    return ResponseEntity.ok("Saved");
  }

  @PostMapping("/vocab/bulk-save")
  @Transactional
  public ResponseEntity<String> bulkSaveVocabMappings(@RequestBody Map<String, String> mappings) {
    for (Map.Entry<String, String> entry : mappings.entrySet()) {
      VocabMapping mapping = vocabRepo.findBySourceVisitType(entry.getKey())
          .orElse(new VocabMapping());
      mapping.setSourceVisitType(entry.getKey());
      mapping.setTargetVisitType(entry.getValue());
      vocabRepo.save(mapping);
    }
    return ResponseEntity.ok("Bulk saved " + mappings.size() + " mappings");
  }

  @DeleteMapping("/vocab/delete/{source}")
  @Transactional
  public ResponseEntity<String> deleteVocabMapping(@PathVariable String source) {
    vocabRepo.deleteBySourceVisitType(source);
    return ResponseEntity.ok("Deleted");
  }

  // ========== HIERARCHY MAPPINGS ==========

  @GetMapping("/hierarchy/all")
  public ResponseEntity<Map<String, String>> getAllHierarchyMappings() {
    List<HierarchyMapping> mappings = hierarchyRepo.findAll();
    Map<String, String> result = new HashMap<>();
    for (HierarchyMapping m : mappings) {
      result.put(m.getSourceVisitType(), m.getParentHierarchyType());
    }
    return ResponseEntity.ok(result);
  }

  @PostMapping("/hierarchy/save")
  @Transactional
  public ResponseEntity<String> saveHierarchyMapping(@RequestBody Map<String, String> payload) {
    String source = payload.get("source");
    String target = payload.get("target");

    if (source == null || source.trim().isEmpty()) {
      return ResponseEntity.badRequest().body("Source visit type required");
    }

    HierarchyMapping mapping = hierarchyRepo.findBySourceVisitType(source)
        .orElse(new HierarchyMapping());
    mapping.setSourceVisitType(source);
    mapping.setParentHierarchyType(target != null ? target : "");
    hierarchyRepo.save(mapping);

    return ResponseEntity.ok("Saved");
  }

  @PostMapping("/hierarchy/bulk-save")
  @Transactional
  public ResponseEntity<String> bulkSaveHierarchyMappings(@RequestBody Map<String, String> mappings) {
    for (Map.Entry<String, String> entry : mappings.entrySet()) {
      HierarchyMapping mapping = hierarchyRepo.findBySourceVisitType(entry.getKey())
          .orElse(new HierarchyMapping());
      mapping.setSourceVisitType(entry.getKey());
      mapping.setParentHierarchyType(entry.getValue());
      hierarchyRepo.save(mapping);
    }
    return ResponseEntity.ok("Bulk saved " + mappings.size() + " mappings");
  }

  @DeleteMapping("/hierarchy/delete/{source}")
  @Transactional
  public ResponseEntity<String> deleteHierarchyMapping(@PathVariable String source) {
    hierarchyRepo.deleteBySourceVisitType(source);
    return ResponseEntity.ok("Deleted");
  }
}