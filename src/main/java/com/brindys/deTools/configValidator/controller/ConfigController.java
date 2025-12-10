package com.brindys.deTools.configValidator.controller;


import com.brindys.deTools.configValidator.model.ValidationResult;
import com.brindys.deTools.configValidator.service.ConfigValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/config")
@CrossOrigin(origins = "*")
public class ConfigController {

  @Autowired
  private ConfigValidator validator;

  @PostMapping("/validate")
  public ResponseEntity<ValidationResult> validateConfig(@RequestBody String config) {
    try {
      ValidationResult result = validator.validateConfig(config);
      return ResponseEntity.ok(result);
    } catch (Exception e) {
      e.printStackTrace();
      return ResponseEntity.internalServerError().build();
    }
  }
}