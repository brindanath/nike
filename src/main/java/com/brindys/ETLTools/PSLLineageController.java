package com.brindys.ETLTools;


import com.brindys.ETLTools.pslFeatureMapper.PSLFeatureMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

@RestController
@RequestMapping("/api")
public class PSLLineageController {

  @PostMapping(value = "/psl-lineage", consumes = "text/plain", produces = "text/plain")
  public ResponseEntity<String> generateLineage(@RequestBody String pslConfig) {
    try {
      PSLFeatureMapper mapper = new PSLFeatureMapper();
      mapper.parse(pslConfig);

      // Capture the output
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos);
      PrintStream old = System.out;
      System.setOut(ps);

      mapper.printLineage();

      System.out.flush();
      System.setOut(old);

      String lineageOutput = baos.toString();
      return ResponseEntity.ok(lineageOutput);

    } catch (Exception e) {
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body("Error generating lineage: " + e.getMessage());
    }
  }
}