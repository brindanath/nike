package com.brindys.deTools.pslFeatureMapper;

import java.util.*;
import java.util.regex.*;

public class PSLFeatureMapper {

  private Map<String, Feature> features = new LinkedHashMap<>();
  private Map<String, Connection> connections = new LinkedHashMap<>();
  private List<Query> queries = new ArrayList<>();
  private Map<String, SchemaDefinition> schemas = new LinkedHashMap<>();
  private Map<String, String> variables = new HashMap<>();
  private String datasetName = "UNKNOWN";
  private String datasetVersion = "UNKNOWN";
  private List<Transform> transforms = new ArrayList<>();
  private List<Hierarchy> hierarchies = new ArrayList<>();



  public void parse(String pslContent) {
    // Remove comments
    pslContent = removeComments(pslContent);

    // Parse dataset info first
    parseDataset(pslContent);

    // Parse variables first (VARIABLE definitions)
    parseVariables(pslContent);

    // Parse feature definitions
    parseFeatures(pslContent);

    // Parse connections
    parseConnections(pslContent);

    // Parse schema definitions (separate from queries)
    parseSchemas(pslContent);

    // Parse query blocks (separate from schemas)
    parseQueries(pslContent);

    // Parse transforms (vocabulary mappings)
    parseTransforms(pslContent);

    // Parse hierarchies
    parseHierarchies(pslContent);
  }


  private void parseTransforms(String content) {
    Pattern transformPattern = Pattern.compile(
        "TRANSFORM\\s+FROM\\s+(\\w+)\\s*\\{([^}]+)\\}",
        Pattern.DOTALL
    );
    Matcher matcher = transformPattern.matcher(content);

    while (matcher.find()) {
      String connectionName = matcher.group(1);
      String transformBody = matcher.group(2);

      // Extract SOURCE.FEATURE
      Pattern featurePattern = Pattern.compile(
          "SOURCE\\.FEATURE\\s*=\\s*\"([^\"]+)\"",
          Pattern.CASE_INSENSITIVE
      );
      Matcher featureMatcher = featurePattern.matcher(transformBody);

      String sourceFeature = null;
      if (featureMatcher.find()) {
        sourceFeature = featureMatcher.group(1);
      }

      if (sourceFeature != null) {
        // Get the connection to find the source
        Connection conn = connections.get(connectionName);
        String vocabularySource = null;

        if (conn != null) {
          if (conn.query != null && !conn.query.isEmpty()) {
            // Extract table from query
            vocabularySource = extractTableFromQuery(conn.query);
          }
        } else {
          // Check if it's a file-based connection
          Pattern fileConnPattern = Pattern.compile(
              "CONNECTION\\s+" + connectionName + "\\s*\\{\\s*FILE\\s*=\\s*([^\\s}]+)",
              Pattern.DOTALL | Pattern.CASE_INSENSITIVE
          );
          Matcher fileConnMatcher = fileConnPattern.matcher(content);
          if (fileConnMatcher.find()) {
            vocabularySource = fileConnMatcher.group(1);
          }
        }

        if (vocabularySource != null) {
          transforms.add(new Transform(sourceFeature, vocabularySource));
        }
      }
    }
  }

  private String extractTableFromQuery(String query) {
    // Look for FROM clause with table name
    Pattern fromPattern = Pattern.compile(
        "FROM\\s+([\\w.]+)",
        Pattern.CASE_INSENSITIVE
    );
    Matcher matcher = fromPattern.matcher(query);

    if (matcher.find()) {
      return matcher.group(1);
    }

    return "UNKNOWN_TABLE";
  }


  private String removeComments(String content) {
    // Remove single-line comments starting with #
    return content.replaceAll("#[^\n]*", "");
  }

  private void parseVariables(String content) {
    Pattern varPattern = Pattern.compile("VARIABLE\\s+(\\w+)\\s*\\{([^}]*)\\}", Pattern.DOTALL);
    Matcher matcher = varPattern.matcher(content);

    while (matcher.find()) {
      String varName = matcher.group(1);
      String varValue = matcher.group(2).trim();
      variables.put(varName, varValue);
    }
  }

  private void parseFeatures(String content) {
    // Match FEATURE lines up until the first CONNECTION or # line with dashes
    Pattern featurePattern = Pattern.compile("FEATURE\\s+(\\w+)\\s*,\\s*([^,]+)\\s*,\\s*(\\w+)(?:\\s*,\\s*(.*))?");

    String[] lines = content.split("\n");
    for (String line : lines) {
      Matcher matcher = featurePattern.matcher(line.trim());
      if (matcher.find()) {
        String featureName = matcher.group(1);
        String description = matcher.group(2).trim();
        String dataType = matcher.group(3);
        String attributes = matcher.group(4) != null ? matcher.group(4).trim() : "";

        features.put(featureName, new Feature(featureName, description, dataType, attributes));
      }

      // Stop at connection definitions
      if (line.trim().startsWith("CONNECTION")) {
        break;
      }
    }
  }

  private void parseHierarchies(String content) {
    Pattern hierarchyPattern = Pattern.compile(
        "HIERARCHY\\s+FROM\\s+(\\w+)\\s*\\{([^}]+)\\}",
        Pattern.DOTALL
    );
    Matcher matcher = hierarchyPattern.matcher(content);

    while (matcher.find()) {
      String connectionName = matcher.group(1);
      String hierarchyBody = matcher.group(2);

      // Extract CHILD.FEATURE
      Pattern childFeaturePattern = Pattern.compile(
          "CHILD\\.FEATURE\\s*=\\s*\"([^\"]+)\"",
          Pattern.CASE_INSENSITIVE
      );
      Matcher childFeatureMatcher = childFeaturePattern.matcher(hierarchyBody);
      String childFeature = null;
      if (childFeatureMatcher.find()) {
        childFeature = childFeatureMatcher.group(1);
      }

      // Extract PARENT.FEATURE
      Pattern parentFeaturePattern = Pattern.compile(
          "PARENT\\.FEATURE\\s*=\\s*\"([^\"]+)\"",
          Pattern.CASE_INSENSITIVE
      );
      Matcher parentFeatureMatcher = parentFeaturePattern.matcher(hierarchyBody);
      String parentFeature = null;
      if (parentFeatureMatcher.find()) {
        parentFeature = parentFeatureMatcher.group(1);
      }

      // Extract CHILD.CODE
      Pattern childCodePattern = Pattern.compile(
          "CHILD\\.CODE\\s*=\\s*(.+?)(?=\\s*$|\\s*PARENT)",
          Pattern.CASE_INSENSITIVE | Pattern.MULTILINE
      );
      Matcher childCodeMatcher = childCodePattern.matcher(hierarchyBody);
      String childCode = null;
      if (childCodeMatcher.find()) {
        childCode = childCodeMatcher.group(1).trim();
      }

      // Extract PARENT.CODE
      Pattern parentCodePattern = Pattern.compile(
          "PARENT\\.CODE\\s*=\\s*(.+?)(?=\\s*$)",
          Pattern.CASE_INSENSITIVE | Pattern.MULTILINE
      );
      Matcher parentCodeMatcher = parentCodePattern.matcher(hierarchyBody);
      String parentCode = null;
      if (parentCodeMatcher.find()) {
        parentCode = parentCodeMatcher.group(1).trim();
      }

      if (childFeature != null && parentFeature != null && childCode != null && parentCode != null) {
        // Get the connection to find the source
        Connection conn = connections.get(connectionName);
        String sourceTable = null;

        if (conn != null && conn.query != null && !conn.query.isEmpty()) {
          // Extract table from query
          sourceTable = extractTableFromQuery(conn.query);
        } else {
          // Check if it's a file-based connection
          Pattern fileConnPattern = Pattern.compile(
              "CONNECTION\\s+" + connectionName + "\\s*\\{\\s*FILE\\s*=\\s*([^\\s}]+)",
              Pattern.DOTALL | Pattern.CASE_INSENSITIVE
          );
          Matcher fileConnMatcher = fileConnPattern.matcher(content);
          if (fileConnMatcher.find()) {
            sourceTable = fileConnMatcher.group(1);
          }
        }

        if (sourceTable != null) {
          // Process child and parent codes to extract column names
          String childColumn = extractColumnFromCode(childCode);
          String parentColumn = extractColumnFromCode(parentCode);

          hierarchies.add(new Hierarchy(
              childFeature,
              parentFeature,
              sourceTable,
              childColumn,
              parentColumn
          ));
        }
      }
    }
  }

  private String extractColumnFromCode(String code) {
    // Check if it's wrapped in *asterisks*
    Pattern columnPattern = Pattern.compile("\\*([^*]+)\\*");
    Matcher matcher = columnPattern.matcher(code);
    if (matcher.find()) {
      return matcher.group(1);
    }
    // Otherwise return as-is (e.g., C1, C2)
    return code;
  }

  private void parseConnections(String content) {
    Pattern connPattern = Pattern.compile(
        "CONNECTION\\s+(\\w+)\\s+FROM\\s+(\\w+)\\s*\\{([^}]+)\\}",
        Pattern.DOTALL
    );
    Matcher matcher = connPattern.matcher(content);

    while (matcher.find()) {
      String connName = matcher.group(1);
      String fromSource = matcher.group(2);
      String connBody = matcher.group(3);

      // Extract QUERY
      Pattern queryPattern = Pattern.compile("QUERY\\s*=\\s*(.+?)(?=\\s*\\}|$)", Pattern.DOTALL);
      Matcher queryMatcher = queryPattern.matcher(connBody);

      String query = "";
      if (queryMatcher.find()) {
        query = queryMatcher.group(1).trim();
        // Substitute variables
        query = substituteVariables(query);
      }

      connections.put(connName, new Connection(connName, fromSource, query));
    }
  }

  private void parseSchemas(String content) {
    // Parse SCHEMA definitions (with optional DEDUPLICATED and PATIENT LEVEL)
    Pattern schemaPattern = Pattern.compile(
        "(DEDUPLICATED\\s+)?(PATIENT\\s+LEVEL\\s+)?SCHEMA\\s+(\\w+)\\s*\\{([^}]+)\\}",
        Pattern.DOTALL
    );
    Matcher matcher = schemaPattern.matcher(content);

    while (matcher.find()) {
      boolean isDeduplicated = matcher.group(1) != null;
      boolean isPatientLevel = matcher.group(2) != null;
      String schemaName = matcher.group(3);
      String schemaBody = matcher.group(4);

      // Parse schema features
      List<String> schemaFeatures = parseSchemaFeatures(schemaBody);

      schemas.put(schemaName, new SchemaDefinition(schemaName, isDeduplicated, isPatientLevel, schemaFeatures));
    }
  }

  private void parseQueries(String content) {
    // Parse QUERY blocks independently of SCHEMA blocks
    Pattern queryPattern = Pattern.compile(
        "QUERY\\s+(\\w+)\\s+FROM\\s+(\\w+)\\s*\\{([^}]+)\\}",
        Pattern.DOTALL
    );
    Matcher matcher = queryPattern.matcher(content);

    while (matcher.find()) {
      String schemaName = matcher.group(1);
      String connectionName = matcher.group(2);
      String queryBody = matcher.group(3);

      // Skip if this is not a valid schema (e.g., it's a hierarchy query)
      SchemaDefinition schema = schemas.get(schemaName);
      if (schema == null) {
        continue; // Not a data schema query
      }

      // Parse query mappings
      Map<String, List<FeatureMapping>> featureMappings = parseQueryMappings(queryBody);

      Query query = new Query(
          schemaName,
          connectionName,
          schema.isDeduplicated,
          schema.isPatientLevel,
          schema.features,
          featureMappings
      );
      queries.add(query);
    }
  }

  private List<String> parseSchemaFeatures(String schemaBody) {
    List<String> features = new ArrayList<>();
    String[] lines = schemaBody.split("\n");
    for (String line : lines) {
      line = line.trim();
      if (!line.isEmpty() && !line.startsWith("//")) {
        features.add(line);
      }
    }
    return features;
  }

  private void parseDataset(String content) {
    Pattern datasetPattern = Pattern.compile(
        "DATASET\\s+(\\w+)\\s*\\{([^}]+)\\}",
        Pattern.DOTALL
    );
    Matcher matcher = datasetPattern.matcher(content);

    if (matcher.find()) {
      datasetName = matcher.group(1);
      String datasetBody = matcher.group(2);

      // Extract DATASET_VERSION
      Pattern versionPattern = Pattern.compile(
          "DATASET_VERSION\\s*=\\s*([^\\s\\n]+)",
          Pattern.CASE_INSENSITIVE
      );
      Matcher versionMatcher = versionPattern.matcher(datasetBody);
      if (versionMatcher.find()) {
        datasetVersion = versionMatcher.group(1);
      }
    }
  }

  private Map<String, List<FeatureMapping>> parseQueryMappings(String queryBody) {
    Map<String, List<FeatureMapping>> mappings = new HashMap<>();
    String[] lines = queryBody.split("\n");

    for (String line : lines) {
      line = line.trim();
      if (line.isEmpty() || line.startsWith("PID") || line.startsWith("ASSERT")) {
        continue;
      }

      // Match patterns like: ICD10 = *diagnosis_cd* or ICD10.START = *condition_start_date*
      Pattern mappingPattern = Pattern.compile("(\\w+)(?:\\.(\\w+))?\\s*=\\s*(.+)");
      Matcher matcher = mappingPattern.matcher(line);

      if (matcher.find()) {
        String feature = matcher.group(1);
        String suffix = matcher.group(2); // Could be START, END, NAME, CODE
        String value = matcher.group(3).trim();

        // Check for NULL
        if (value.equals("NULL")) {
          if (!mappings.containsKey(feature)) {
            mappings.put(feature, new ArrayList<>());
          }
          mappings.get(feature).add(new FeatureMapping(null, "NULL", suffix));
          continue;
        }

        // Check for literal string values in quotes
        Pattern literalPattern = Pattern.compile("\"([^\"]+)\"");
        Matcher literalMatcher = literalPattern.matcher(value);

        if (literalMatcher.find()) {
          String literalValue = literalMatcher.group(1);
          if (!mappings.containsKey(feature)) {
            mappings.put(feature, new ArrayList<>());
          }
          // Only add CODE suffix mappings to avoid duplicate entries for NAME/CODE
          if (!"NAME".equals(suffix)) {
            mappings.get(feature).add(new FeatureMapping(null, "\"" + literalValue + "\"", suffix));
          }
          continue;
        }

        // Extract column reference from *column* or MAP(..., *column*)
        Pattern columnPattern = Pattern.compile("\\*([^*]+)\\*");
        Matcher columnMatcher = columnPattern.matcher(value);

        if (columnMatcher.find()) {
          String column = columnMatcher.group(1);
          if (!mappings.containsKey(feature)) {
            mappings.put(feature, new ArrayList<>());
          }

          // Add the mapping, but track whether it's a NAME or CODE suffix
          // Only add CODE suffix mappings to avoid duplicate entries
          if (!"NAME".equals(suffix)) {
            mappings.get(feature).add(new FeatureMapping(column, value, suffix));
          }
        }
      }
    }

    return mappings;
  }

  private String substituteVariables(String text) {
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      text = text.replace("$" + entry.getKey(), entry.getValue());
    }
    return text;
  }

  public void printLineage() {
    System.out.println("=".repeat(80));
    System.out.println(datasetName + " " + datasetVersion + " FEATURE LINEAGE MAP");
    System.out.println("=".repeat(80));
    System.out.println();

    // Group queries by schema name
    Map<String, List<Query>> schemaGroups = new LinkedHashMap<>();
    for (Query query : queries) {
      schemaGroups.computeIfAbsent(query.schemaName, k -> new ArrayList<>()).add(query);
    }

    // Print each schema group
    for (Map.Entry<String, List<Query>> entry : schemaGroups.entrySet()) {
      String schemaName = entry.getKey();
      List<Query> schemaQueries = entry.getValue();

      // Get schema type from first query (they should all be the same)
      Query firstQuery = schemaQueries.get(0);
      StringBuilder schemaType = new StringBuilder();
      if (firstQuery.isDeduplicated) schemaType.append("DEDUPLICATED ");
      if (firstQuery.isPatientLevel) schemaType.append("PATIENT LEVEL ");
      String schemaTypeStr = schemaType.length() > 0 ? " (" + schemaType.toString().trim() + ")" : "";

      System.out.println("SCHEMA: " + schemaName + schemaTypeStr);

      // Group queries by connection name to consolidate duplicates
      Map<String, List<Query>> connectionGroups = new LinkedHashMap<>();
      for (Query query : schemaQueries) {
        connectionGroups.computeIfAbsent(query.connectionName, k -> new ArrayList<>()).add(query);
      }

      // Print each connection group
      int connectionIndex = 0;
      for (Map.Entry<String, List<Query>> connEntry : connectionGroups.entrySet()) {
        String connectionName = connEntry.getKey();
        List<Query> connectionQueries = connEntry.getValue();

        connectionIndex++;
        boolean isLastConnection = (connectionIndex == connectionGroups.size());
        String connPrefix = isLastConnection ? "└─" : "├─";
        String featurePrefix = isLastConnection ? "   " : "│  ";

        System.out.println(connPrefix + " CONNECTION: " + connectionName);
        System.out.println(featurePrefix + " │");

        Connection conn = connections.get(connectionName);
        if (conn == null) continue;

        // Parse source tables from connection query
        Map<String, String> tableAliases = parseTableAliases(conn.query);

        // Merge all feature mappings from all queries using this connection
        Map<String, Set<FeatureMapping>> consolidatedMappings = new LinkedHashMap<>();
        List<String> schemaFeatures = connectionQueries.get(0).schemaFeatures;

        for (Query query : connectionQueries) {
          for (Map.Entry<String, List<FeatureMapping>> mappingEntry : query.featureMappings.entrySet()) {
            String feature = mappingEntry.getKey();
            if (!consolidatedMappings.containsKey(feature)) {
              consolidatedMappings.put(feature, new LinkedHashSet<>());
            }
            consolidatedMappings.get(feature).addAll(mappingEntry.getValue());
          }
        }

        // Get all features that are actually mapped
        Set<String> mappedFeatures = consolidatedMappings.keySet();

        int featureCount = 0;
        for (String feature : schemaFeatures) {
          if (!mappedFeatures.contains(feature)) {
            continue; // Skip features not mapped
          }

          featureCount++;
          boolean isLastFeature = (featureCount == mappedFeatures.size());
          String featureBranch = isLastFeature ? "└───" : "├───";
          String sourcePrefix = isLastFeature ? "    " : "│   ";

          Set<FeatureMapping> mappings = consolidatedMappings.get(feature);

          System.out.println(featurePrefix + " " + featureBranch + " FEATURE: " + feature);

          if (mappings != null && !mappings.isEmpty()) {
            List<FeatureMapping> mappingList = new ArrayList<>(mappings);
            for (int j = 0; j < mappingList.size(); j++) {
              FeatureMapping mapping = mappingList.get(j);
              boolean isLastMapping = (j == mappingList.size() - 1);
              String mappingBranch = isLastMapping ? "└─>" : "├─>";

              if (mapping.column == null) {
                // Check if it's a literal string value (starts and ends with quotes)
                if (mapping.rawValue.startsWith("\"") && mapping.rawValue.endsWith("\"")) {
                  String suffixStr = mapping.suffix != null ? " (" + mapping.suffix + ")" : "";
                  System.out.println(featurePrefix + " " + sourcePrefix + " " + mappingBranch + " SOURCE: " + mapping.rawValue + suffixStr);
                } else {
                  // It's NULL or some other special value
                  System.out.println(featurePrefix + " " + sourcePrefix + " " + mappingBranch + " SOURCE: " + mapping.rawValue);
                }
              } else {
                String sourceTable = resolveSourceTable(mapping.column, tableAliases, conn.query);
                String suffixStr = mapping.suffix != null ? " (" + mapping.suffix + ")" : "";
                System.out.println(featurePrefix + " " + sourcePrefix + " " + mappingBranch + " SOURCE: " + sourceTable + "." + mapping.column + suffixStr);
              }
            }
          }
        }

        if (!isLastConnection) {
          System.out.println(featurePrefix);
        }
      }

      System.out.println();
    }

    // ============== ADD VOCABULARY SECTION HERE ==============
    // Print vocabulary section
    if (!transforms.isEmpty()) {
      System.out.println("=".repeat(80));
      System.out.println("VOCABULARY MAPPINGS");
      System.out.println("=".repeat(80));
      System.out.println();

      for (int i = 0; i < transforms.size(); i++) {
        Transform transform = transforms.get(i);
        boolean isLast = (i == transforms.size() - 1);
        String prefix = isLast ? "└───" : "├───";

        System.out.println(prefix + " FEATURE: " + transform.feature);
        System.out.println((isLast ? "    " : "│   ") + " └─> VOCABULARY SOURCE: " + transform.vocabularySource);

        if (!isLast) {
          System.out.println("│");
        }
      }
      System.out.println();
    }
    if (!hierarchies.isEmpty()) {
      System.out.println("=".repeat(80));
      System.out.println("FEATURE HIERARCHIES");
      System.out.println("=".repeat(80));
      System.out.println();

      for (int i = 0; i < hierarchies.size(); i++) {
        Hierarchy hierarchy = hierarchies.get(i);
        boolean isLast = (i == hierarchies.size() - 1);
        String prefix = isLast ? "└───" : "├───";
        String indent = isLast ? "    " : "│   ";

        System.out.println(prefix + " HIERARCHY: " + hierarchy.parentFeature + " to " + hierarchy.childFeature);
        System.out.println(indent + " ├─> CHILD.CODE = " + hierarchy.sourceTable + "." + hierarchy.childColumn);
        System.out.println(indent + " └─> PARENT.CODE = " + hierarchy.sourceTable + "." + hierarchy.parentColumn);

        if (!isLast) {
          System.out.println("│");
        }
      }
      System.out.println();
    }

    // =========================================================
  }

  private Map<String, String> parseTableAliases(String query) {
    Map<String, String> aliases = new HashMap<>();

    // Match patterns like: FROM schema.table alias or FROM schema.table AS alias
    Pattern fromPattern = Pattern.compile(
        "FROM\\s+([\\w.]+)\\s+(?:AS\\s+)?(\\w+)",
        Pattern.CASE_INSENSITIVE
    );
    Matcher matcher = fromPattern.matcher(query);

    while (matcher.find()) {
      String table = matcher.group(1);
      String alias = matcher.group(2);
      if (!alias.equalsIgnoreCase("WHERE") && !alias.equalsIgnoreCase("LEFT") &&
          !alias.equalsIgnoreCase("INNER") && !alias.equalsIgnoreCase("RIGHT") &&
          !alias.equalsIgnoreCase("JOIN")) {
        aliases.put(alias, table);
      }
    }

    // Parse JOINs with balanced parentheses for subqueries
    int i = 0;
    while (i < query.length()) {
      Pattern joinStart = Pattern.compile(
          "(?:LEFT|RIGHT|INNER|OUTER)?\\s*JOIN\\s+",
          Pattern.CASE_INSENSITIVE
      );
      Matcher joinMatcher = joinStart.matcher(query.substring(i));

      if (!joinMatcher.find()) {
        break;
      }

      int joinPos = i + joinMatcher.end();

      // Skip whitespace
      while (joinPos < query.length() && Character.isWhitespace(query.charAt(joinPos))) {
        joinPos++;
      }

      if (joinPos < query.length() && query.charAt(joinPos) == '(') {
        // Extract balanced parentheses
        int parenCount = 1;
        int start = joinPos + 1;
        int end = start;

        while (end < query.length() && parenCount > 0) {
          if (query.charAt(end) == '(') parenCount++;
          if (query.charAt(end) == ')') parenCount--;
          end++;
        }

        String subquery = query.substring(start, end - 1);

        // Get alias after closing paren
        Pattern aliasPattern = Pattern.compile(
            "^\\s*(?:AS\\s+)?(\\w+)",
            Pattern.CASE_INSENSITIVE
        );
        Matcher aliasMatcher = aliasPattern.matcher(query.substring(end));

        if (aliasMatcher.find()) {
          String alias = aliasMatcher.group(1);
          String innerTable = extractTableFromSubquery(subquery);
          if (innerTable != null) {
            aliases.put(alias, innerTable);
          }
        }

        i = end;
      } else {
        i = joinPos + 1;
      }
    }

    return aliases;
  }

  private String extractTableFromSubquery(String subquery) {
    Pattern fromPattern = Pattern.compile(
        "FROM\\s+([\\w.]+)",
        Pattern.CASE_INSENSITIVE
    );
    Matcher matcher = fromPattern.matcher(subquery);

    if (matcher.find()) {
      return matcher.group(1);
    }

    return null;
  }

  private String findColumnAliasInSelect(String column, String query) {
    Pattern selectPattern = Pattern.compile(
        "SELECT\\s+(.+?)\\s+FROM",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );
    Matcher matcher = selectPattern.matcher(query);

    if (!matcher.find()) {
      return null;
    }

    String selectClause = matcher.group(1);

    Pattern columnPattern = Pattern.compile(
        "(\\w+)\\.(\\w+)(?:\\s+(?:as\\s+)?(\\w+))?",
        Pattern.CASE_INSENSITIVE
    );
    Matcher colMatcher = columnPattern.matcher(selectClause);

    while (colMatcher.find()) {
      String alias = colMatcher.group(1);
      String actualColumn = colMatcher.group(2);
      String asName = colMatcher.group(3);

      if (actualColumn.equalsIgnoreCase(column) ||
          (asName != null && asName.equalsIgnoreCase(column))) {
        return alias;
      }
    }

    return null;
  }

  private String resolveSourceTable(String column, Map<String, String> tableAliases, String fullQuery) {
    if (column.contains(".")) {
      String alias = column.substring(0, column.indexOf("."));
      String table = tableAliases.get(alias);
      return table != null ? table : "UNKNOWN_TABLE";
    }

    String aliasForColumn = findColumnAliasInSelect(column, fullQuery);
    if (aliasForColumn != null && tableAliases.containsKey(aliasForColumn)) {
      return tableAliases.get(aliasForColumn);
    }

    Pattern fromPattern = Pattern.compile(
        "FROM\\s+([\\w.]+)(?:\\s+(?:AS\\s+)?\\w+)?",
        Pattern.CASE_INSENSITIVE
    );
    Matcher matcher = fromPattern.matcher(fullQuery);
    if (matcher.find()) {
      return matcher.group(1);
    }

    return "UNKNOWN_TABLE";
  }




  // Inner classes
  static class Feature {
    String name;
    String description;
    String dataType;
    String attributes;

    Feature(String name, String description, String dataType, String attributes) {
      this.name = name;
      this.description = description;
      this.dataType = dataType;
      this.attributes = attributes;
    }
  }

  static class Hierarchy {
    String childFeature;
    String parentFeature;
    String sourceTable;
    String childColumn;
    String parentColumn;

    Hierarchy(String childFeature, String parentFeature, String sourceTable,
        String childColumn, String parentColumn) {
      this.childFeature = childFeature;
      this.parentFeature = parentFeature;
      this.sourceTable = sourceTable;
      this.childColumn = childColumn;
      this.parentColumn = parentColumn;
    }
  }




  static class Transform {
    String feature;
    String vocabularySource;

    Transform(String feature, String vocabularySource) {
      this.feature = feature;
      this.vocabularySource = vocabularySource;
    }
  }


  static class Connection {
    String name;
    String fromSource;
    String query;

    Connection(String name, String fromSource, String query) {
      this.name = name;
      this.fromSource = fromSource;
      this.query = query;
    }
  }

  static class SchemaDefinition {
    String name;
    boolean isDeduplicated;
    boolean isPatientLevel;
    List<String> features;

    SchemaDefinition(String name, boolean isDeduplicated, boolean isPatientLevel, List<String> features) {
      this.name = name;
      this.isDeduplicated = isDeduplicated;
      this.isPatientLevel = isPatientLevel;
      this.features = features;
    }
  }

  static class Query {
    String schemaName;
    String connectionName;
    boolean isDeduplicated;
    boolean isPatientLevel;
    List<String> schemaFeatures;
    Map<String, List<FeatureMapping>> featureMappings;

    Query(String schemaName, String connectionName, boolean isDeduplicated, boolean isPatientLevel,
        List<String> schemaFeatures, Map<String, List<FeatureMapping>> featureMappings) {
      this.schemaName = schemaName;
      this.connectionName = connectionName;
      this.isDeduplicated = isDeduplicated;
      this.isPatientLevel = isPatientLevel;
      this.schemaFeatures = schemaFeatures;
      this.featureMappings = featureMappings;
    }
  }



  static class FeatureMapping {
    String column;
    String rawValue;
    String suffix;

    FeatureMapping(String column, String rawValue, String suffix) {
      this.column = column;
      this.rawValue = rawValue;
      this.suffix = suffix;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FeatureMapping that = (FeatureMapping) o;
      return Objects.equals(column, that.column) &&
          Objects.equals(rawValue, that.rawValue) &&
          Objects.equals(suffix, that.suffix);
    }

    @Override
    public int hashCode() {
      return Objects.hash(column, rawValue, suffix);
    }
  }

}