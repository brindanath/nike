package com.brindys.deTools.visitTypeMapper;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {
    "com.brindys.deTools.visitTypeMapper",
    "com.brindys.deTools.configValidator"
})
public class VisitTypeToolsApplication implements CommandLineRunner {

  @Value("${server.port}")
  private String serverPort;

  public static void main(String[] args) {
    SpringApplication.run(VisitTypeToolsApplication.class, args);
  }

  @Override
  public void run(String... args) {
    System.out.println("========================================");
    System.out.println("Visit Type Tools Server Running!");
    System.out.println("Access at: http://localhost:" + serverPort);
    System.out.println("========================================");
  }
}