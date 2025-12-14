package com.brindys.deTools;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {
    "com.brindys.deTools",  // Add this to scan the base package
    "com.brindys.deTools.visitTypeMapper",
    "com.brindys.deTools.configValidator"
})
public class DeToolsApplication implements CommandLineRunner {

  @Value("${server.port}")
  private String serverPort;

  public static void main(String[] args) {
    SpringApplication.run(DeToolsApplication.class, args);
  }

  @Override
  public void run(String... args) {
    System.out.println("========================================");
    System.out.println("DE Tools Server Running!");
    System.out.println("Access at: http://localhost:" + serverPort);
    System.out.println("========================================");
    System.out.println("Available Tools:");
    System.out.println("  - PSL Release Notes Generator");
    System.out.println("  - Visit Type Mapper");
    System.out.println("  - Config Formatter");
    System.out.println("  - Config Checker");
    System.out.println("  - PSL Feature Lineage Mapper");
    System.out.println("========================================");
  }
}