package com.brindys.ETLTools.configValidator.service;



import com.brindys.ETLTools.configValidator.model.ValidationIssue;
import com.brindys.ETLTools.configValidator.model.ValidationResult;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class ConfigValidator {

  public ValidationResult validateConfig(String config) {
    ValidationResult result = new ValidationResult();

    // Run all validation rules
    result.getErrors().addAll(checkMissingBrackets(config));
    result.getErrors().addAll(checkMissingParentheses(config));
    result.getWarnings().addAll(checkDuplicateCacheNames(config));
    /*
    result.getErrors().addAll(checkBrokenConnections(config));
    result.getWarnings().addAll(checkUndefinedVariables(config));
    result.getWarnings().addAll(checkUndefinedFeatures(config));
    result.getErrors().addAll(checkMissingRequiredFields(config));
    result.getWarnings().addAll(checkUnusedFeatures(config));
    result.getWarnings().addAll(checkLongQueries(config));

*/


    // Calculate summary
    result.setTotalIssues(result.getErrors().size() + result.getWarnings().size());
    result.setSummary(generateSummary(result));

    return result;
  }

  // Rule 1: Check for missing/unmatched brackets
  public List<ValidationIssue> checkMissingBrackets(String config) {
    List<ValidationIssue> issues = new ArrayList<>();
    String[] lines = config.split("\n");
    Stack<BracketInfo> stack = new Stack<>();

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      int lineNum = i + 1;

      for (int j = 0; j < line.length(); j++) {
        char ch = line.charAt(j);
        if (ch == '{') {
          stack.push(new BracketInfo(lineNum, j, ch));
        } else if (ch == '}') {
          if (stack.isEmpty()) {
            issues.add(new ValidationIssue(
                "ERROR",
                "SYNTAX",
                lineNum,
                "Closing bracket '}' found without matching opening bracket",
                "Add an opening bracket '{' before this line or remove this closing bracket",
                line.trim()
            ));
          } else {
            stack.pop();
          }
        }
      }
    }

    // Check for unclosed brackets
    while (!stack.isEmpty()) {
      BracketInfo bracket = stack.pop();
      issues.add(new ValidationIssue(
          "ERROR",
          "SYNTAX",
          bracket.lineNumber,
          "Opening bracket '{' never closed",
          "Add a closing bracket '}' to match this opening bracket",
          lines[bracket.lineNumber - 1].trim()
      ));
    }

    return issues;
  }

  // Rule 2: Check for missing/unmatched parentheses
  public List<ValidationIssue> checkMissingParentheses(String config) {
    List<ValidationIssue> issues = new ArrayList<>();
    String[] lines = config.split("\n");

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      int lineNum = i + 1;

      // Skip comments
      if (line.trim().startsWith("#") || line.trim().startsWith("/*")) {
        continue;
      }

      int openCount = 0;
      int closeCount = 0;

      for (char ch : line.toCharArray()) {
        if (ch == '(') openCount++;
        if (ch == ')') closeCount++;
      }

      if (openCount != closeCount) {
        issues.add(new ValidationIssue(
            "ERROR",
            "SYNTAX",
            lineNum,
            "Mismatched parentheses: " + openCount + " opening, " + closeCount + " closing",
            "Check PSL syntax and ensure all parentheses are properly matched",
            line.trim()
        ));
      }
    }

    return issues;
  }

  // Rule 3: Check for duplicate cache names
  public List<ValidationIssue> checkDuplicateCacheNames(String config) {
    List<ValidationIssue> issues = new ArrayList<>();
    Map<String, Integer> cacheNames = new HashMap<>();
    String[] lines = config.split("\n");

    Pattern cachePattern = Pattern.compile("CACHE\\s*=\\s*([\\w.]+)");

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      int lineNum = i + 1;

      Matcher matcher = cachePattern.matcher(line);
      if (matcher.find()) {
        String cacheName = matcher.group(1);

        if (cacheNames.containsKey(cacheName)) {
          issues.add(new ValidationIssue(
              "WARNING",
              "LOGIC",
              lineNum,
              "Duplicate cache name: '" + cacheName + "' (first seen on line " + cacheNames.get(cacheName) + ")",
              "Use unique cache names to avoid overwriting. Change one of them.",
              line.trim()
          ));
        } else {
          cacheNames.put(cacheName, lineNum);
        }
      }
    }

    return issues;
  }
/*
  // Rule 4: Check for broken "COMMECTIONS" (typo for CONNECTIONS)
  public List<ValidationIssue> checkBrokenConnections(String config) {
    List<ValidationIssue> issues = new ArrayList<>();
    String[] lines = config.split("\n");

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      int lineNum = i + 1;

      // Check for COMMECTION typo
      if (line.contains("COMMECTION")) {
        issues.add(new ValidationIssue(
            "ERROR",
            "SYNTAX",
            lineNum,
            "Typo detected: 'COMMECTION' should be 'CONNECTION'",
            "Change 'COMMECTION' to 'CONNECTION'",
            line.trim()
        ));
      }

      // Check for CONNECTION FROM references that don't exist
      Pattern connectionFromPattern = Pattern.compile("CONNECTION\\s+(\\w+)\\s+FROM\\s+(\\w+)");
      Matcher matcher = connectionFromPattern.matcher(line);

      if (matcher.find()) {
        String parentConnection = matcher.group(2);
        if (!connectionExists(config, parentConnection)) {
          issues.add(new ValidationIssue(
              "ERROR",
              "LOGIC",
              lineNum,
              "Connection references undefined parent: '" + parentConnection + "'",
              "Define CONNECTION " + parentConnection + " before referencing it, or fix the name",
              line.trim()
          ));
        }
      }
    }

    return issues;
  }

  // Rule 5: Check for undefined variables
  public List<ValidationIssue> checkUndefinedVariables(String config) {
    List<ValidationIssue> issues = new ArrayList<>();
    Set<String> definedVariables = new HashSet<>();
    String[] lines = config.split("\n");

    // First pass: collect all defined variables
    Pattern varPattern = Pattern.compile("VARIABLE\\s+(\\w+)");
    for (String line : lines) {
      Matcher matcher = varPattern.matcher(line);
      if (matcher.find()) {
        definedVariables.add(matcher.group(1));
      }
    }

    // Second pass: check for usage of undefined variables
    Pattern usagePattern = Pattern.compile("\\$([\\w.]+)");
    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      int lineNum = i + 1;

      Matcher matcher = usagePattern.matcher(line);
      while (matcher.find()) {
        String varName = matcher.group(1);
        // Remove any schema prefixes like DATA_SCHEMA.table -> DATA_SCHEMA
        String baseVar = varName.split("\\.")[0];

        if (!definedVariables.contains(baseVar) && !varName.startsWith("*")) {
          issues.add(new ValidationIssue(
              "WARNING",
              "LOGIC",
              lineNum,
              "Variable '$" + baseVar + "' used but not defined",
              "Add 'VARIABLE " + baseVar + " { ... }' or fix the variable name",
              line.trim()
          ));
        }
      }
    }

    return issues;
  }

  // Rule 6: Check for undefined features in queries
  public List<ValidationIssue> checkUndefinedFeatures(String config) {
    List<ValidationIssue> issues = new ArrayList<>();
    Set<String> definedFeatures = new HashSet<>();
    String[] lines = config.split("\n");

    // First pass: collect all defined features
    Pattern featurePattern = Pattern.compile("FEATURE\\s+(\\w+)\\s*,");
    for (String line : lines) {
      Matcher matcher = featurePattern.matcher(line);
      if (matcher.find()) {
        definedFeatures.add(matcher.group(1));
      }
    }

    // Second pass: check for feature usage in QUERY blocks
    boolean inQuery = false;
    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      int lineNum = i + 1;

      if (line.trim().startsWith("QUERY")) {
        inQuery = true;
      } else if (line.trim().startsWith("}") && inQuery) {
        inQuery = false;
      }

      if (inQuery && line.contains("=")) {
        // Extract feature name (left side of =)
        String[] parts = line.split("=");
        if (parts.length > 0) {
          String featurePart = parts[0].trim();
          // Remove .CODE, .NAME, .START, .END suffixes
          String featureName = featurePart.split("\\.")[0].trim();

          if (!featureName.isEmpty() &&
              !featureName.equals("PID") &&
              !featureName.equals("DOB") &&
              !featureName.equals("ASSERT") &&
              !featureName.equals("FOR") &&
              !definedFeatures.contains(featureName)) {

            issues.add(new ValidationIssue(
                "WARNING",
                "LOGIC",
                lineNum,
                "Feature '" + featureName + "' used in QUERY but not defined",
                "Add 'FEATURE " + featureName + ", ...' or fix the feature name",
                line.trim()
            ));
          }
        }
      }
    }

    return issues;
  }

  // Rule 7: Check for missing required fields in DATASET and QUERY
  public List<ValidationIssue> checkMissingRequiredFields(String config) {
    List<ValidationIssue> issues = new ArrayList<>();
    String[] lines = config.split("\n");

    // Check DATASET has PRECISION
    boolean hasDataset = false;
    boolean hasPrecision = false;
    int datasetLine = -1;

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      int lineNum = i + 1;

      if (line.trim().startsWith("DATASET")) {
        hasDataset = true;
        datasetLine = lineNum;
      }

      if (hasDataset && line.contains("PRECISION")) {
        hasPrecision = true;
      }

      if (hasDataset && line.trim().equals("}")) {
        if (!hasPrecision) {
          issues.add(new ValidationIssue(
              "ERROR",
              "LOGIC",
              datasetLine,
              "DATASET missing required field: PRECISION",
              "Add 'PRECISION = DAY' (or HOUR/MINUTE) inside DATASET block",
              lines[datasetLine - 1].trim()
          ));
        }
        hasDataset = false;
        hasPrecision = false;
      }
    }

    // Check QUERY has PID and at least one date
    boolean inQuery = false;
    boolean hasPID = false;
    boolean hasDate = false;
    int queryLine = -1;

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      int lineNum = i + 1;

      if (line.trim().startsWith("QUERY")) {
        inQuery = true;
        queryLine = lineNum;
        hasPID = false;
        hasDate = false;
      }

      if (inQuery) {
        if (line.contains("PID")) hasPID = true;
        if (line.contains("_DATE") || line.contains("DOB")) hasDate = true;
      }

      if (inQuery && line.trim().equals("}")) {
        if (!hasPID) {
          issues.add(new ValidationIssue(
              "ERROR",
              "LOGIC",
              queryLine,
              "QUERY missing required field: PID",
              "Add 'PID = ...' assignment in QUERY block",
              lines[queryLine - 1].trim()
          ));
        }
        if (!hasDate) {
          issues.add(new ValidationIssue(
              "WARNING",
              "LOGIC",
              queryLine,
              "QUERY missing date field - at least one date field is typically required",
              "Add a date field assignment (e.g., DIAGNOSIS_DATE = ...)",
              lines[queryLine - 1].trim()
          ));
        }
        inQuery = false;
      }
    }

    return issues;
  }

  // Rule 8: Check for unused features
  public List<ValidationIssue> checkUnusedFeatures(String config) {
    List<ValidationIssue> issues = new ArrayList<>();
    Map<String, Integer> definedFeatures = new HashMap<>();
    Set<String> usedFeatures = new HashSet<>();
    String[] lines = config.split("\n");

    // Collect defined features
    Pattern featurePattern = Pattern.compile("FEATURE\\s+(\\w+)\\s*,");
    for (int i = 0; i < lines.length; i++) {
      Matcher matcher = featurePattern.matcher(lines[i]);
      if (matcher.find()) {
        definedFeatures.put(matcher.group(1), i + 1);
      }
    }

    // Find used features (in SCHEMA, QUERY, CSV blocks)
    for (String line : lines) {
      for (String feature : definedFeatures.keySet()) {
        if (line.contains(feature) && !line.trim().startsWith("FEATURE " + feature)) {
          usedFeatures.add(feature);
        }
      }
    }

    // Report unused features
    for (Map.Entry<String, Integer> entry : definedFeatures.entrySet()) {
      if (!usedFeatures.contains(entry.getKey())) {
        issues.add(new ValidationIssue(
            "WARNING",
            "BEST_PRACTICE",
            entry.getValue(),
            "Feature '" + entry.getKey() + "' is defined but never used",
            "Remove this feature definition or add it to a SCHEMA/QUERY",
            lines[entry.getValue() - 1].trim()
        ));
      }
    }

    return issues;
  }

  // Rule 9: Check for extremely long queries
  public List<ValidationIssue> checkLongQueries(String config) {
    List<ValidationIssue> issues = new ArrayList<>();
    String[] lines = config.split("\n");

    boolean inQuery = false;
    int queryStartLine = -1;
    StringBuilder queryContent = new StringBuilder();

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      int lineNum = i + 1;

      if (line.trim().contains("QUERY=") || line.trim().contains("QUERY =")) {
        inQuery = true;
        queryStartLine = lineNum;
        queryContent = new StringBuilder();
      }

      if (inQuery) {
        queryContent.append(line).append("\n");
      }

      // End of CONNECTION block or next section
      if (inQuery && (line.trim().equals("}") || line.trim().startsWith("CONNECTION") || line.trim().startsWith("QUERY ") || line.trim().startsWith("MAP"))) {
        if (queryContent.length() > 3000) {
          issues.add(new ValidationIssue(
              "WARNING",
              "BEST_PRACTICE",
              queryStartLine,
              "Query is very long (" + queryContent.length() + " chars) - consider breaking it up or simplifying",
              "Split into multiple queries or move complex logic to views/CTEs in the database",
              "QUERY at line " + queryStartLine
          ));
        }
        inQuery = false;
      }
    }

    return issues;
  }
*/
  // Helper methods
  private boolean connectionExists(String config, String connectionName) {
    Pattern pattern = Pattern.compile("CONNECTION\\s+" + Pattern.quote(connectionName) + "\\s+[{FROM]");
    return pattern.matcher(config).find();
  }

  private String generateSummary(ValidationResult result) {
    int errors = result.getErrors().size();
    int warnings = result.getWarnings().size();

    if (errors == 0 && warnings == 0) {
      return "✅ No issues found! Config looks good.";
    } else if (errors == 0) {
      return "⚠️ Found " + warnings + " warning(s). Config is valid but could be improved.";
    } else {
      return "❌ Found " + errors + " error(s) and " + warnings + " warning(s). Fix errors before running ETL.";
    }
  }

  // Helper class for bracket tracking
  private static class BracketInfo {
    int lineNumber;
    int position;
    char bracket;

    BracketInfo(int lineNumber, int position, char bracket) {
      this.lineNumber = lineNumber;
      this.position = position;
      this.bracket = bracket;
    }
  }
}