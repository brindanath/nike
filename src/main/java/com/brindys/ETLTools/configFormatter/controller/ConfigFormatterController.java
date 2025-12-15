package com.brindys.ETLTools.configFormatter.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class ConfigFormatterController {

  @GetMapping("/config-formatter")
  public String configFormatter() {
    return "config-formatter";
  }
}