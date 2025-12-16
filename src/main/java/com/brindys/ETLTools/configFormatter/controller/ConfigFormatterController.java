package com.brindys.ETLTools.configFormatter.controller;

import com.brindys.ETLTools.configFormatter.service.ConfigFormatterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/config-formatter")
public class ConfigFormatterController {

  @Autowired
  private ConfigFormatterService formatterService;

  @PostMapping("/format")
  public ResponseEntity<String> formatConfig(@RequestBody String config) {
    try {
      String formatted = formatterService.formatConfig(config);
      return ResponseEntity.ok(formatted);
    } catch (Exception e) {
      return ResponseEntity.badRequest().body("Error formatting config: " + e.getMessage());
    }
  }
}