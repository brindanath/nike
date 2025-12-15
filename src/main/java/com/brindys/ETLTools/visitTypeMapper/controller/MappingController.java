package com.brindys.ETLTools.visitTypeMapper.controller;

import com.brindys.ETLTools.visitTypeMapper.model.HierarchyMapping;
import com.brindys.ETLTools.visitTypeMapper.model.VocabMapping;
import com.brindys.ETLTools.visitTypeMapper.repository.HierarchyMappingRepository;
import com.brindys.ETLTools.visitTypeMapper.repository.VocabMappingRepository;

import com.brindys.ETLTools.support.github.GitHubService;
import com.brindys.ETLTools.support.github.dto.CommitRequest;
import com.brindys.ETLTools.support.github.dto.CommitResponse;
import com.brindys.ETLTools.support.github.dto.MappingHistory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;  // ADD THIS
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

  // ADD THIS NEW AUTOWIRED FIELD:
  @Autowired
  private GitHubService gitHubService;

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

  @PostMapping("/github/commit")
  public ResponseEntity<CommitResponse> commitToGitHub(@RequestBody CommitRequest request) {
    CommitResponse response = gitHubService.commitMapping(request);

    if (response.isSuccess()) {
      return ResponseEntity.ok(response);
    } else {
      return ResponseEntity.status(500).body(response);
    }
  }

  /**
   * Get mapping history from GitHub
   */
  @GetMapping("/github/history")
  public ResponseEntity<?> getMappingHistory() {
    try {
      List<MappingHistory> history = gitHubService.getMappingHistory();
      return ResponseEntity.ok(history);
    } catch (IOException e) {
      return ResponseEntity.status(500).body(Map.of(
          "success", false,
          "error", "Failed to fetch history: " + e.getMessage()
      ));
    }
  }

  /**
   * Test GitHub connection
   */
  @GetMapping("/github/status")
  public ResponseEntity<Map<String, String>> getGitHubStatus() {
    try {
      String info = gitHubService.getRepositoryInfo();
      return ResponseEntity.ok(Map.of("status", info));
    } catch (IOException e) {
      return ResponseEntity.status(500).body(Map.of(
          "status", "Error connecting to GitHub: " + e.getMessage()
      ));
    }
  }
  /**
   * Restore a mapping from GitHub
   */
  @PostMapping("/github/restore")
  public ResponseEntity<?> restoreMapping(@RequestBody Map<String, String> request) {
    try {
      String filename = request.get("filename");
      Map<String, String> mapping = gitHubService.loadMapping(filename);

      return ResponseEntity.ok(Map.of(
          "success", true,
          "mapping", mapping,
          "message", "Mapping loaded from GitHub"
      ));
    } catch (IOException e) {
      return ResponseEntity.status(500).body(Map.of(
          "success", false,
          "error", e.getMessage()
      ));
    }
  }


  /**
   * Helper endpoint to get all current mappings as JSON for committing
   */
  @GetMapping("/github/current-mappings")
  public ResponseEntity<Map<String, Object>> getCurrentMappings() {
    Map<String, Object> result = new HashMap<>();

    // Get vocab mappings
    List<VocabMapping> vocabs = vocabRepo.findAll();
    Map<String, String> vocabMap = new HashMap<>();
    for (VocabMapping v : vocabs) {
      vocabMap.put(v.getSourceVisitType(), v.getTargetVisitType());
    }

    // Get hierarchy mappings
    List<HierarchyMapping> hierarchies = hierarchyRepo.findAll();
    Map<String, String> hierarchyMap = new HashMap<>();
    for (HierarchyMapping h : hierarchies) {
      hierarchyMap.put(h.getSourceVisitType(), h.getParentHierarchyType());
    }

    result.put("vocab_mappings", vocabMap);
    result.put("hierarchy_mappings", hierarchyMap);
    result.put("total_count", vocabMap.size() + hierarchyMap.size());

    return ResponseEntity.ok(result);
  }


}