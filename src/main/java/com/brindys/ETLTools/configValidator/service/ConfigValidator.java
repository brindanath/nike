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
    ConfigSections sections = new ConfigSections(config);

    // Extract defined features (needed for multiple rules)
    Set<String> definedFeatures = extractDefinedFeatures(config);

    // Global rules (whole config)
    result.getErrors().addAll(checkMissingBrackets(config));
    result.getErrors().addAll(checkMissingParentheses(config));
    result.getWarnings().addAll(checkCommentedOutBlocks(config));
    result.getWarnings().addAll(checkFeatureNameMappings(config));
    result.getErrors().addAll(checkDatasetMetadata(config));

    // Rules for above hierarchy
    result.getWarnings().addAll(checkDuplicateCacheNames(sections.aboveHierarchy));
    result.getErrors().addAll(checkInconsistentConnectionSources(sections.aboveHierarchy));
    result.getErrors().addAll(checkConnectionSortColumns(sections.aboveHierarchy));

    // Rules for below hierarchy
    result.getWarnings().addAll(checkRequiredBelowHierarchyDefinitions(sections.belowHierarchy));
    result.getWarnings().addAll(checkFeatureHierarchiesWithDefinedFeatures(config, sections.belowHierarchy, definedFeatures));

    result.setTotalIssues(result.getErrors().size() + result.getWarnings().size());
    result.setSummary(generateSummary(result));

    return result;
  }





  private static class ConfigSections {
    String fullConfig;
    String aboveHierarchy;
    String belowHierarchy;
    int hierarchyLineNumber;

    ConfigSections(String config) {
      this.fullConfig = config;
      String[] lines = config.split("\n");

      for (int i = 0; i < lines.length; i++) {
        String line = lines[i].trim().toUpperCase();
        if (line.startsWith("#") && line.contains("HIER")) {
          this.hierarchyLineNumber = i + 1;
          this.aboveHierarchy = String.join("\n", Arrays.copyOfRange(lines, 0, i));
          this.belowHierarchy = String.join("\n", Arrays.copyOfRange(lines, i + 1, lines.length));
          return;
        }
      }

      // No hierarchy line found - treat entire config as "above"
      this.hierarchyLineNumber = -1;
      this.aboveHierarchy = config;
      this.belowHierarchy = "";
    }
  }





  // Rule 1: Check for missing/unmatched brackets
  public List<ValidationIssue> checkMissingBrackets(String config) {
    List<ValidationIssue> issues = new ArrayList<>();
    String[] lines = config.split("\n");
    Stack<BracketInfo> stack = new Stack<>();
    boolean inString = false;
    char stringChar = '"';

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      int lineNum = i + 1;

      for (int j = 0; j < line.length(); j++) {
        char ch = line.charAt(j);

        // Track string literals to ignore brackets inside them
        if ((ch == '"' || ch == '\'') && (j == 0 || line.charAt(j - 1) != '\\')) {
          if (!inString) {
            inString = true;
            stringChar = ch;
          } else if (ch == stringChar) {
            inString = false;
          }
        }

        if (!inString) {
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

  // Rule 2: Check for missing/unmatched parentheses (context-aware)
  public List<ValidationIssue> checkMissingParentheses(String config) {
    List<ValidationIssue> issues = new ArrayList<>();
    String[] lines = config.split("\n");

    // Track parentheses across blocks (CONNECTION, QUERY, etc.)
    Pattern blockStartPattern = Pattern.compile(
        "^\\s*(CONNECTION|QUERY|TRANSFORM|SCHEMA|HIERARCHY|TQL|CSV|DATASET|FEATURE)\\s+",
        Pattern.CASE_INSENSITIVE
    );

    boolean inBlock = false;
    int blockStartLine = -1;
    int parenCount = 0;
    int blockBraceDepth = 0;
    StringBuilder blockContent = new StringBuilder();
    boolean inMultiLineComment = false;
    boolean inString = false;
    char stringChar = '"';

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      int lineNum = i + 1;
      String trimmedLine = line.trim();

      // Track multi-line comments
      if (trimmedLine.contains("/*")) {
        inMultiLineComment = true;
      }
      if (inMultiLineComment) {
        if (trimmedLine.contains("*/")) {
          inMultiLineComment = false;
        }
        continue;
      }

      // Skip single-line comments
      if (trimmedLine.startsWith("#") || trimmedLine.startsWith("//")) {
        continue;
      }

      // Check if we're starting a new block
      if (blockStartPattern.matcher(trimmedLine).find()) {
        // If we were in a previous block, check it
        if (inBlock && parenCount != 0) {
          issues.add(new ValidationIssue(
              "ERROR",
              "SYNTAX",
              blockStartLine,
              "Mismatched parentheses in block: " + Math.abs(parenCount) + " " +
                  (parenCount > 0 ? "unclosed opening" : "extra closing") + " parentheses",
              "Check SQL queries and PSL expressions for properly matched parentheses across multiple lines",
              "Block starting at line " + blockStartLine
          ));
        }

        // Start tracking new block
        inBlock = true;
        blockStartLine = lineNum;
        parenCount = 0;
        blockBraceDepth = 0;
        blockContent = new StringBuilder();
      }

      if (inBlock) {
        blockContent.append(line).append("\n");

        // Count parentheses, ignoring those in strings
        inString = false;
        for (int j = 0; j < line.length(); j++) {
          char ch = line.charAt(j);

          // Track string literals
          if ((ch == '"' || ch == '\'') && (j == 0 || line.charAt(j - 1) != '\\')) {
            if (!inString) {
              inString = true;
              stringChar = ch;
            } else if (ch == stringChar) {
              inString = false;
            }
          }

          if (!inString) {
            if (ch == '(') parenCount++;
            if (ch == ')') parenCount--;
            if (ch == '{') blockBraceDepth++;
            if (ch == '}') blockBraceDepth--;
          }
        }

        // When block closes (brace depth returns to 0), validate
        if (blockBraceDepth == 0 && trimmedLine.contains("}")) {
          if (parenCount != 0) {
            issues.add(new ValidationIssue(
                "ERROR",
                "SYNTAX",
                blockStartLine,
                "Mismatched parentheses in block: " + Math.abs(parenCount) + " " +
                    (parenCount > 0 ? "unclosed opening" : "extra closing") + " parentheses",
                "Check SQL queries and PSL expressions for properly matched parentheses across multiple lines",
                "Block starting at line " + blockStartLine
            ));
          }
          inBlock = false;
          parenCount = 0;
        }
      }
    }

    // Check if we ended while still in a block
    if (inBlock && parenCount != 0) {
      issues.add(new ValidationIssue(
          "ERROR",
          "SYNTAX",
          blockStartLine,
          "Mismatched parentheses in block: " + Math.abs(parenCount) + " " +
              (parenCount > 0 ? "unclosed opening" : "extra closing") + " parentheses",
          "Check SQL queries and PSL expressions for properly matched parentheses across multiple lines",
          "Block starting at line " + blockStartLine
      ));
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


  // Rule 4: Check for inconsistent CONNECTION sources above hierarchy line
  public List<ValidationIssue> checkInconsistentConnectionSources(String configAboveHierarchy) {
    List<ValidationIssue> issues = new ArrayList<>();
    String[] lines = configAboveHierarchy.split("\n");

    Map<String, Integer> sourceCount = new HashMap<>();
    Map<String, List<ConnectionInfo>> connectionsBySource = new HashMap<>();

    Pattern connectionPattern = Pattern.compile("CONNECTION\\s+(\\w+)\\s+FROM\\s+(\\w+)", Pattern.CASE_INSENSITIVE);

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i].trim();
      int lineNum = i + 1;

      Matcher matcher = connectionPattern.matcher(line);
      if (matcher.find()) {
        String connectionName = matcher.group(1);
        String source = matcher.group(2);

        sourceCount.put(source, sourceCount.getOrDefault(source, 0) + 1);

        connectionsBySource
            .computeIfAbsent(source, k -> new ArrayList<>())
            .add(new ConnectionInfo(connectionName, source, lineNum, line));
      }
    }

    // If there's only one source or no connections, no issue
    if (sourceCount.size() <= 1) {
      return issues;
    }

    // Find the most common source (the one that should be used)
    String expectedSource = sourceCount.entrySet().stream()
        .max(Map.Entry.comparingByValue())
        .map(Map.Entry::getKey)
        .orElse("");

    // Flag any connections that don't use the expected source
    for (Map.Entry<String, List<ConnectionInfo>> entry : connectionsBySource.entrySet()) {
      String source = entry.getKey();

      if (!source.equals(expectedSource)) {
        for (ConnectionInfo conn : entry.getValue()) {
          issues.add(new ValidationIssue(
              "ERROR",
              "LOGIC",
              conn.lineNumber,
              "Inconsistent CONNECTION source: '" + source + "' (expected '" + expectedSource + "')",
              "Change 'FROM " + source + "' to 'FROM " + expectedSource + "' to match other connections",
              conn.line
          ));
        }
      }
    }

    return issues;
  }

  // Rule 5: Check that all CONNECTION blocks have a valid SORT COLUMN
  public List<ValidationIssue> checkConnectionSortColumns(String configAboveHierarchy) {
    List<ValidationIssue> issues = new ArrayList<>();
    String[] lines = configAboveHierarchy.split("\n");

    Pattern connectionPattern = Pattern.compile("CONNECTION\\s+(\\w+)\\s+FROM\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
    Pattern sortColumnPattern = Pattern.compile("SORT\\s+COLUMN\\s*=\\s*\\*\\s*(\\w+)\\s*\\*", Pattern.CASE_INSENSITIVE);
    Pattern emptySortPattern = Pattern.compile("SORT\\s+COLUMN\\s*=\\s*$", Pattern.CASE_INSENSITIVE);

    String currentConnection = null;
    int connectionLineNum = -1;
    String connectionLine = "";
    boolean inConnection = false;
    boolean foundSortColumn = false;
    int braceDepth = 0;

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      int lineNum = i + 1;
      String trimmedLine = line.trim();

      // Track CONNECTION blocks
      Matcher connectionMatcher = connectionPattern.matcher(trimmedLine);
      if (connectionMatcher.find()) {
        currentConnection = connectionMatcher.group(1);
        connectionLineNum = lineNum;
        connectionLine = trimmedLine;
        inConnection = true;
        foundSortColumn = false;
        braceDepth = 0;
      }

      if (inConnection) {
        // Track braces to know when we exit the CONNECTION block
        for (char ch : line.toCharArray()) {
          if (ch == '{') braceDepth++;
          if (ch == '}') braceDepth--;
        }

        // Check for empty SORT COLUMN
        Matcher emptyMatcher = emptySortPattern.matcher(trimmedLine);
        if (emptyMatcher.find()) {
          issues.add(new ValidationIssue(
              "ERROR",
              "SYNTAX",
              lineNum,
              "CONNECTION '" + currentConnection + "' has empty SORT COLUMN value",
              "Add a column name in the format: SORT COLUMN = *column_name*",
              trimmedLine
          ));
          foundSortColumn = true; // Mark as found (even though invalid) to avoid duplicate error
        }

        // Check for valid SORT COLUMN
        Matcher sortMatcher = sortColumnPattern.matcher(trimmedLine);
        if (sortMatcher.find()) {
          foundSortColumn = true;
        }

        // When we exit the CONNECTION block, check if SORT COLUMN was found
        if (braceDepth == 0 && trimmedLine.contains("}")) {
          if (!foundSortColumn) {
            issues.add(new ValidationIssue(
                "ERROR",
                "SYNTAX",
                connectionLineNum,
                "CONNECTION '" + currentConnection + "' is missing SORT COLUMN declaration",
                "Add 'SORT COLUMN = *column_name*' inside the CONNECTION block",
                connectionLine
            ));
          }
          inConnection = false;
          currentConnection = null;
        }
      }
    }

    return issues;
  }

  // Rule 6: Check for commented-out configuration blocks (global)
  public List<ValidationIssue> checkCommentedOutBlocks(String config) {
    List<ValidationIssue> issues = new ArrayList<>();
    String[] lines = config.split("\n", -1); // -1 to preserve empty lines

    Pattern keywordPattern = Pattern.compile(
        "\\b(CONNECTION|SCHEMA|QUERY|TRANSFORM|HIERARCHY|TQL|CSV)\\b",
        Pattern.CASE_INSENSITIVE
    );

    // Pattern to match section header lines like #----------------------------HIERARCHIES-------------------------------------
    Pattern sectionHeaderPattern = Pattern.compile("^#+\\s*-+.*-+\\s*$");

    boolean foundCommentedBlock = false;
    List<Integer> commentedLineNumbers = new ArrayList<>();
    boolean inMultiLineComment = false;

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      String trimmedLine = line.trim();
      int lineNum = i + 1;

      // Check for multi-line comment start
      if (trimmedLine.contains("/*")) {
        inMultiLineComment = true;
      }

      // If we're in any type of comment, check for keywords
      boolean isCommented = false;

      if (inMultiLineComment) {
        isCommented = true;
      } else if (trimmedLine.startsWith("#") || trimmedLine.startsWith("//")) {
        isCommented = true;
      }

      if (isCommented) {
        // Skip section header lines (e.g., #----------------------------DB CONNECTION-----)
        if (!sectionHeaderPattern.matcher(trimmedLine).matches()) {
          Matcher matcher = keywordPattern.matcher(line);
          if (matcher.find()) {
            foundCommentedBlock = true;
            commentedLineNumbers.add(lineNum);
          }
        }
      }

      // Check for multi-line comment end
      if (trimmedLine.contains("*/")) {
        inMultiLineComment = false;
      }
    }

    // Add single warning if any commented blocks were found
    if (foundCommentedBlock) {
      String lineNumbersStr;
      if (commentedLineNumbers.size() <= 5) {
        lineNumbersStr = commentedLineNumbers.toString();
      } else {
        lineNumbersStr = commentedLineNumbers.subList(0, 5) + "... (+" + (commentedLineNumbers.size() - 5) + " more)";
      }

      issues.add(new ValidationIssue(
          "WARNING",
          "CLEANUP",
          commentedLineNumbers.get(0), // Report on first occurrence
          "Found commented-out configuration blocks on lines: " + lineNumbersStr,
          "Remove commented-out code blocks to keep config clean and maintainable",
          "Multiple lines contain commented CONNECTION, SCHEMA, QUERY, TRANSFORM, HIERARCHY, TQL, or CSV blocks"
      ));
    }

    return issues;
  }

  // Rule 7: Check for required TQL and CSV definitions below hierarchy line
  public List<ValidationIssue> checkRequiredBelowHierarchyDefinitions(String configBelowHierarchy) {
    List<ValidationIssue> issues = new ArrayList<>();

    if (configBelowHierarchy == null || configBelowHierarchy.trim().isEmpty()) {
      // If there's no content below hierarchy, add all warnings
      issues.add(new ValidationIssue(
          "WARNING",
          "MISSING_DEFINITION",
          -1,
          "Missing TQL UTILIZATION definition below hierarchy line",
          "Add a TQL UTILIZATION block to define how features are utilized in the dataset",
          "No content found below hierarchy line"
      ));
      issues.add(new ValidationIssue(
          "ERROR",
          "MISSING_DEFINITION",
          -1,
          "Missing TQL DATASET_DATE definition below hierarchy line",
          "Add a TQL DATASET_DATE block to define the date range for the dataset",
          "No content found below hierarchy line"
      ));
      issues.add(new ValidationIssue(
          "WARNING",
          "MISSING_DEFINITION",
          -1,
          "Missing CSV definitions below hierarchy line",
          "Add CSV blocks to define column mappings for your features",
          "No content found below hierarchy line"
      ));
      return issues;
    }

    String[] lines = configBelowHierarchy.split("\n");

    Pattern tqlUtilizationPattern = Pattern.compile("TQL\\s+UTILIZATION\\s*\\{", Pattern.CASE_INSENSITIVE);
    Pattern tqlDatasetDatePattern = Pattern.compile("TQL\\s+DATASET_DATE\\s*\\{", Pattern.CASE_INSENSITIVE);
    Pattern csvPattern = Pattern.compile("CSV\\s+\\S+\\s*\\{", Pattern.CASE_INSENSITIVE);

    boolean foundUtilization = false;
    boolean foundDatasetDate = false;
    boolean foundCsv = false;

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i].trim();

      // Skip commented lines
      if (line.startsWith("#") || line.startsWith("//")) {
        continue;
      }

      if (tqlUtilizationPattern.matcher(line).find()) {
        foundUtilization = true;
      }

      if (tqlDatasetDatePattern.matcher(line).find()) {
        foundDatasetDate = true;
      }

      if (csvPattern.matcher(line).find()) {
        foundCsv = true;
      }
    }

    // Add warnings for missing definitions
    if (!foundUtilization) {
      issues.add(new ValidationIssue(
          "WARNING",
          "MISSING_DEFINITION",
          -1,
          "Missing TQL UTILIZATION definition below hierarchy line",
          "Add a TQL UTILIZATION block to define how features are utilized in the dataset",
          "Example: TQL UTILIZATION { UNION($DIAGNOSES, $PROCEDURES, $MEDICATIONS) }"
      ));
    }

    if (!foundDatasetDate) {
      issues.add(new ValidationIssue(
          "WARNING",
          "MISSING_DEFINITION",
          -1,
          "Missing TQL DATASET_DATE definition below hierarchy line",
          "Add a TQL DATASET_DATE block to define the date range for the dataset",
          "Example: TQL DATASET_DATE { DATE(\"1890-01-01\", \"2025-06-30\") }"
      ));
    }

    if (!foundCsv) {
      issues.add(new ValidationIssue(
          "WARNING",
          "MISSING_DEFINITION",
          -1,
          "Missing CSV definitions below hierarchy line",
          "Add CSV blocks to define column mappings for your features",
          "Example: CSV RX.NDC { C3 = ROUTE=$ROUTE.CODE }"
      ));
    }

    return issues;
  }

  // Rule 8: Check that required features have name mappings (global)
  public List<ValidationIssue> checkFeatureNameMappings(String config) {
    List<ValidationIssue> issues = new ArrayList<>();
    String[] lines = config.split("\n");

    // Features that require name mappings if they exist
    Set<String> featuresThatNeedNames = new HashSet<>(Arrays.asList(
        "ICD9", "ICD10", "ICD10PCS", "CPT", "LOINC", "RX", "NDC"
    ));

    // First pass: Find which features are actually defined
    Pattern featurePattern = Pattern.compile("^\\s*FEATURE\\s+(\\w+)\\s*,", Pattern.CASE_INSENSITIVE);
    Set<String> definedFeatures = new HashSet<>();

    boolean inMultiLineComment = false;

    for (String line : lines) {
      String trimmedLine = line.trim();

      // Track multi-line comments
      if (trimmedLine.contains("/*")) {
        inMultiLineComment = true;
      }

      // Skip comments
      if (inMultiLineComment || trimmedLine.startsWith("#") || trimmedLine.startsWith("//")) {
        if (trimmedLine.contains("*/")) {
          inMultiLineComment = false;
        }
        continue;
      }

      Matcher featureMatcher = featurePattern.matcher(trimmedLine);
      if (featureMatcher.find()) {
        String feature = featureMatcher.group(1).toUpperCase();
        if (featuresThatNeedNames.contains(feature)) {
          definedFeatures.add(feature);
        }
      }
    }

    // If no relevant features are defined, return early
    if (definedFeatures.isEmpty()) {
      return issues;
    }

    // Second pass: Check which defined features have name mappings
    Set<String> featuresWithNames = new HashSet<>();

    // Pattern to match FEATURE.NAME in QUERY blocks (must be at start of assignment, not nested in functions)
    Pattern queryNamePattern = Pattern.compile("^\\s*(\\w+)\\.NAME\\s*=", Pattern.CASE_INSENSITIVE);

    // Patterns for TRANSFORM blocks
    Pattern transformStartPattern = Pattern.compile("TRANSFORM\\s+FROM", Pattern.CASE_INSENSITIVE);
    Pattern sourceFeaturePattern = Pattern.compile("SOURCE\\.FEATURE\\s*=\\s*[\"']([^\"']+)[\"']", Pattern.CASE_INSENSITIVE);
    Pattern targetNamePattern = Pattern.compile("TARGET\\.NAME\\s*=", Pattern.CASE_INSENSITIVE);

    boolean inTransform = false;
    String currentTransformFeature = null;
    boolean currentTransformHasTargetName = false;
    int braceDepth = 0;
    inMultiLineComment = false;

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      String trimmedLine = line.trim();

      // Track multi-line comments
      if (trimmedLine.contains("/*")) {
        inMultiLineComment = true;
      }

      // Skip comments
      if (inMultiLineComment || trimmedLine.startsWith("#") || trimmedLine.startsWith("//")) {
        if (trimmedLine.contains("*/")) {
          inMultiLineComment = false;
        }
        continue;
      }

      // Check for FEATURE.NAME in queries
      Matcher queryNameMatcher = queryNamePattern.matcher(line);
      if (queryNameMatcher.find()) {
        String feature = queryNameMatcher.group(1).toUpperCase();
        if (definedFeatures.contains(feature)) {
          featuresWithNames.add(feature);
        }
      }

      // Track TRANSFORM blocks
      if (transformStartPattern.matcher(trimmedLine).find()) {
        inTransform = true;
        currentTransformFeature = null;
        currentTransformHasTargetName = false;
        braceDepth = 0;
      }

      if (inTransform) {
        // Track braces
        for (char ch : line.toCharArray()) {
          if (ch == '{') braceDepth++;
          if (ch == '}') braceDepth--;
        }

        // Check for SOURCE.FEATURE
        Matcher sourceFeatureMatcher = sourceFeaturePattern.matcher(trimmedLine);
        if (sourceFeatureMatcher.find()) {
          currentTransformFeature = sourceFeatureMatcher.group(1).toUpperCase();
        }

        // Check for TARGET.NAME
        if (targetNamePattern.matcher(trimmedLine).find()) {
          currentTransformHasTargetName = true;
        }

        // When transform block closes, check if we found both SOURCE.FEATURE and TARGET.NAME
        if (braceDepth == 0 && trimmedLine.contains("}")) {
          if (currentTransformFeature != null &&
              currentTransformHasTargetName &&
              definedFeatures.contains(currentTransformFeature)) {
            featuresWithNames.add(currentTransformFeature);
          }
          inTransform = false;
          currentTransformFeature = null;
          currentTransformHasTargetName = false;
        }
      }
    }

    // Check which defined features are missing name mappings
    for (String feature : definedFeatures) {
      if (!featuresWithNames.contains(feature)) {
        issues.add(new ValidationIssue(
            "WARNING",
            "MISSING_MAPPING",
            -1,
            "Feature '" + feature + "' is missing a name mapping",
            "Add either: (1) " + feature + ".NAME = *column* in a QUERY block, or (2) a TRANSFORM with SOURCE.FEATURE = \"" + feature + "\" and TARGET.NAME",
            "Feature requires name mapping for proper vocabulary resolution"
        ));
      }
    }

    return issues;
  }

  // Rule 9: Check that required features have hierarchy definitions (below hierarchy line)
  public List<ValidationIssue> checkFeatureHierarchiesWithDefinedFeatures(
      String config, String configBelowHierarchy, Set<String> definedFeatures) {

    List<ValidationIssue> issues = new ArrayList<>();

    if (configBelowHierarchy == null || configBelowHierarchy.trim().isEmpty()) {
      return issues; // No content below hierarchy
    }

    // Features that require hierarchies
    Set<String> featuresThatNeedHierarchies = new HashSet<>(Arrays.asList(
        "ICD9", "ICD10", "ICD10PCS", "RX", "ATC", "VISIT_TYPE"
    ));

    // Filter to only features that are defined AND need hierarchies
    Set<String> relevantFeatures = new HashSet<>();
    for (String feature : definedFeatures) {
      if (featuresThatNeedHierarchies.contains(feature)) {
        relevantFeatures.add(feature);
      }
    }

    if (relevantFeatures.isEmpty()) {
      return issues; // No relevant features to check
    }

    // Get all found hierarchies
    Set<String> foundHierarchies = extractHierarchies(configBelowHierarchy);

    // Check if ATC is defined - special case
    boolean hasATC = relevantFeatures.contains("ATC");
    boolean hasRX = relevantFeatures.contains("RX");

    if (hasATC) {
      // Need all three ATC-related hierarchies
      if (!foundHierarchies.contains("RX->RX")) {
        issues.add(new ValidationIssue(
            "WARNING",
            "MISSING_HIERARCHY",
            -1,
            "Missing RX to RX hierarchy (required when ATC is defined)",
            "Add: HIERARCHY FROM RXNORM_TO_RXNORM { CHILD.FEATURE = \"RX\" PARENT.FEATURE = \"RX\" ... }",
            "ATC feature requires three hierarchies: RX->RX, RX->ATC, and ATC->ATC"
        ));
      }
      if (!foundHierarchies.contains("RX->ATC")) {
        issues.add(new ValidationIssue(
            "WARNING",
            "MISSING_HIERARCHY",
            -1,
            "Missing RX to ATC hierarchy (required when ATC is defined)",
            "Add: HIERARCHY FROM ATC_TO_RXNORM { CHILD.FEATURE = \"RX\" PARENT.FEATURE = \"ATC\" ... }",
            "ATC feature requires three hierarchies: RX->RX, RX->ATC, and ATC->ATC"
        ));
      }
      if (!foundHierarchies.contains("ATC->ATC")) {
        issues.add(new ValidationIssue(
            "WARNING",
            "MISSING_HIERARCHY",
            -1,
            "Missing ATC to ATC hierarchy (required when ATC is defined)",
            "Add: HIERARCHY FROM ATC_HIER { CHILD.FEATURE = \"ATC\" PARENT.FEATURE = \"ATC\" ... }",
            "ATC feature requires three hierarchies: RX->RX, RX->ATC, and ATC->ATC"
        ));
      }
    } else if (hasRX) {
      // RX without ATC - just needs RX->RX
      if (!foundHierarchies.contains("RX->RX")) {
        issues.add(new ValidationIssue(
            "WARNING",
            "MISSING_HIERARCHY",
            -1,
            "Missing RX to RX hierarchy",
            "Add: HIERARCHY FROM RXNORM_TO_RXNORM { CHILD.FEATURE = \"RX\" PARENT.FEATURE = \"RX\" ... }",
            "RX feature requires a hierarchy definition"
        ));
      }
    }

    // Check other features (ICD9, ICD10, ICD10PCS, VISIT_TYPE)
    for (String feature : relevantFeatures) {
      if (!feature.equals("ATC") && !feature.equals("RX")) {
        String hierarchyKey = feature + "->" + feature;
        if (!foundHierarchies.contains(hierarchyKey)) {
          issues.add(new ValidationIssue(
              "WARNING",
              "MISSING_HIERARCHY",
              -1,
              "Missing hierarchy for feature '" + feature + "'",
              "Add: HIERARCHY FROM " + feature + "_HIER { CHILD.FEATURE = \"" + feature + "\" PARENT.FEATURE = \"" + feature + "\" ... }",
              "Feature requires a hierarchy definition"
          ));
        }
      }
    }

    return issues;
  }

  // Helper to extract hierarchies from config
  private Set<String> extractHierarchies(String configBelowHierarchy) {
    Set<String> foundHierarchies = new HashSet<>();
    String[] lines = configBelowHierarchy.split("\n");

    Pattern hierarchyStartPattern = Pattern.compile("HIERARCHY\\s+FROM", Pattern.CASE_INSENSITIVE);
    Pattern childFeaturePattern = Pattern.compile("CHILD\\.FEATURE\\s*=\\s*[\"']([^\"']+)[\"']", Pattern.CASE_INSENSITIVE);
    Pattern parentFeaturePattern = Pattern.compile("PARENT\\.FEATURE\\s*=\\s*[\"']([^\"']+)[\"']", Pattern.CASE_INSENSITIVE);

    boolean inHierarchy = false;
    String currentChildFeature = null;
    String currentParentFeature = null;
    int braceDepth = 0;
    boolean inMultiLineComment = false;

    for (String line : lines) {
      String trimmedLine = line.trim();

      // Track multi-line comments
      if (trimmedLine.contains("/*")) {
        inMultiLineComment = true;
      }

      if (inMultiLineComment) {
        if (trimmedLine.contains("*/")) {
          inMultiLineComment = false;
        }
        continue;
      }

      // Skip single-line comments
      if (trimmedLine.startsWith("#") || trimmedLine.startsWith("//")) {
        continue;
      }

      // Track HIERARCHY blocks
      if (hierarchyStartPattern.matcher(trimmedLine).find()) {
        inHierarchy = true;
        currentChildFeature = null;
        currentParentFeature = null;
        braceDepth = 0;
      }

      if (inHierarchy) {
        // Track braces
        for (char ch : line.toCharArray()) {
          if (ch == '{') braceDepth++;
          if (ch == '}') braceDepth--;
        }

        // Check for CHILD.FEATURE
        Matcher childMatcher = childFeaturePattern.matcher(trimmedLine);
        if (childMatcher.find()) {
          currentChildFeature = childMatcher.group(1).toUpperCase();
        }

        // Check for PARENT.FEATURE
        Matcher parentMatcher = parentFeaturePattern.matcher(trimmedLine);
        if (parentMatcher.find()) {
          currentParentFeature = parentMatcher.group(1).toUpperCase();
        }

        // When hierarchy block closes, record the relationship
        if (braceDepth == 0 && trimmedLine.contains("}")) {
          if (currentChildFeature != null && currentParentFeature != null) {
            foundHierarchies.add(currentChildFeature + "->" + currentParentFeature);
          }
          inHierarchy = false;
          currentChildFeature = null;
          currentParentFeature = null;
        }
      }
    }

    return foundHierarchies;
  }

  // Helper method to extract defined features
  private Set<String> extractDefinedFeatures(String config) {
    Set<String> definedFeatures = new HashSet<>();
    String[] lines = config.split("\n");
    Pattern featurePattern = Pattern.compile("^\\s*FEATURE\\s+(\\w+)\\s*,", Pattern.CASE_INSENSITIVE);
    boolean inMultiLineComment = false;

    for (String line : lines) {
      String trimmedLine = line.trim();

      if (trimmedLine.contains("/*")) {
        inMultiLineComment = true;
      }

      if (inMultiLineComment || trimmedLine.startsWith("#") || trimmedLine.startsWith("//")) {
        if (trimmedLine.contains("*/")) {
          inMultiLineComment = false;
        }
        continue;
      }

      Matcher featureMatcher = featurePattern.matcher(trimmedLine);
      if (featureMatcher.find()) {
        definedFeatures.add(featureMatcher.group(1).toUpperCase());
      }
    }

    return definedFeatures;
  }

  // Rule 10: Check that DATASET metadata block is complete and properly defined
  public List<ValidationIssue> checkDatasetMetadata(String config) {
    List<ValidationIssue> issues = new ArrayList<>();
    String[] lines = config.split("\n");

    // Required fields in DATASET block
    Set<String> requiredFields = new HashSet<>(Arrays.asList(
        "PRECISION",
        "COMPRESSION",
        "STATISTICS",
        "DATA_PROVIDER_CODE",
        "DATA_SOURCE_CODE",
        "DATASET_VERSION",
        "DATASET_EFFECTIVE_DATE",
        "DATASET_DESCRIPTION_JSON",
        "DATASET_STATISTICS_JSON",
        "FUTURE_CUTOFF",
        "SOURCE_RX_CLAIMS",
        "SOURCE_MEDICAL_CLAIMS",
        "SOURCE_OMOP",
        "SOURCE_EHR",
        "GEOGRAPHIC_ENTITY_TYPE"
    ));

    Set<String> foundFields = new HashSet<>();
    Set<String> emptyFields = new HashSet<>();

    Pattern datasetStartPattern = Pattern.compile("^\\s*DATASET\\s+(\\w+)\\s*\\{", Pattern.CASE_INSENSITIVE);
    Pattern fieldPattern = Pattern.compile("^\\s*(\\w+)\\s*=\\s*(.*)$");

    boolean inDataset = false;
    boolean foundDatasetBlock = false;
    int datasetLineNum = -1;
    int braceDepth = 0;
    boolean inMultiLineComment = false;

    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      String trimmedLine = line.trim();
      int lineNum = i + 1;

      // Track multi-line comments
      if (trimmedLine.contains("/*")) {
        inMultiLineComment = true;
      }

      if (inMultiLineComment) {
        if (trimmedLine.contains("*/")) {
          inMultiLineComment = false;
        }
        continue;
      }

      // Skip single-line comments
      if (trimmedLine.startsWith("#") || trimmedLine.startsWith("//")) {
        continue;
      }

      // Check for DATASET block start
      Matcher datasetMatcher = datasetStartPattern.matcher(trimmedLine);
      if (datasetMatcher.find()) {
        inDataset = true;
        foundDatasetBlock = true;
        datasetLineNum = lineNum;
        braceDepth = 0;
      }

      if (inDataset) {
        // Track braces
        for (char ch : line.toCharArray()) {
          if (ch == '{') braceDepth++;
          if (ch == '}') braceDepth--;
        }

        // Check for field definitions
        Matcher fieldMatcher = fieldPattern.matcher(trimmedLine);
        if (fieldMatcher.find()) {
          String fieldName = fieldMatcher.group(1).toUpperCase();
          String fieldValue = fieldMatcher.group(2).trim();

          if (requiredFields.contains(fieldName)) {
            foundFields.add(fieldName);

            // Check if field is empty or undefined
            if (fieldValue.isEmpty() || fieldValue.equals("=")) {
              emptyFields.add(fieldName);
              issues.add(new ValidationIssue(
                  "ERROR",
                  "MISSING_VALUE",
                  lineNum,
                  "DATASET field '" + fieldName + "' is defined but has no value",
                  "Provide a value for " + fieldName,
                  trimmedLine
              ));
            }
          }
        }

        // When DATASET block closes, check for missing fields
        if (braceDepth == 0 && trimmedLine.contains("}")) {
          inDataset = false;
          break; // Stop after first DATASET block
        }
      }
    }

    // Check if DATASET block exists at all
    if (!foundDatasetBlock) {
      issues.add(new ValidationIssue(
          "ERROR",
          "MISSING_BLOCK",
          -1,
          "Missing DATASET metadata block at the top of config",
          "Add a DATASET block with all required metadata fields",
          "DATASET block should be the first element in the config"
      ));
      return issues; // No point checking for missing fields if block doesn't exist
    }

    // Check for missing required fields
    for (String requiredField : requiredFields) {
      if (!foundFields.contains(requiredField)) {
        issues.add(new ValidationIssue(
            "ERROR",
            "MISSING_FIELD",
            datasetLineNum,
            "DATASET block is missing required field: " + requiredField,
            "Add '" + requiredField + " = <value>' inside the DATASET block",
            "Required field not found in DATASET metadata"
        ));
      }
    }

    return issues;
  }



  // Helper class for connection tracking
  private static class ConnectionInfo {
    String name;
    String source;
    int lineNumber;
    String line;

    ConnectionInfo(String name, String source, int lineNumber, String line) {
      this.name = name;
      this.source = source;
      this.lineNumber = lineNumber;
      this.line = line;
    }
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