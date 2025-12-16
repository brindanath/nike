package com.brindys.ETLTools;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLFormatterScratch {

  public static void main(String[] args) {
    // Test cases - add more as you go
    List<String> testQueries = new ArrayList<>();

    // Test 1: Simple query
    testQueries.add("""
            SELECT
                ptid,
                diag_date,
                concept_code,
                concept_name
            FROM $DATA_SCHEMA.ascvd_diag_icd10
        """);

    // Test 2: Query with CAST
    testQueries.add("""
            SELECT 
                CAST(person_id AS string) AS person_id,
                provider_id,
                code, 
                CAST(event_date AS date) AS procedure_date
            FROM $DATA_SCHEMA.procedure p
            INNER JOIN $DATA_SCHEMA.atropos_concepts c
            ON c.concept_code = p.code
            WHERE LEFT(person_id, 1) = '1'
            AND c.vocabulary_id IN ('CPT4', 'HCPCS')
            AND c.concept_class_id != 'CPT4 Hierarchy'
        """);

    // Test 3: Complex query with subquery
    testQueries.add("""
            SELECT
                d.ptid,
                d.diag_date,
                d.diagnosis_cd,
                e.provid
            from $DATA_SCHEMA.optum_ehr_202508__diag d
                LEFT JOIN (
            SELECT
                encid,
                provid,
                ROW_NUMBER() OVER (
                    PARTITION BY encid
                    ORDER BY
                        CASE
                            WHEN provider_role = 'ATTENDING' THEN 1
                            WHEN provider_role = 'ADMITTING' THEN 2
                            ELSE 3
                        END,
                        provid
                ) as rn
            FROM
                $DATA_SCHEMA.optum_ehr_202508__enc_prov
        ) e
        ON
            d.encid = e.encid AND e.rn = 1
        WHERE
            d.diagnosis_cd_type = 'ICD10'
        """);

    // Run tests
    for (int i = 0; i < testQueries.size(); i++) {
      System.out.println("========== TEST " + (i + 1) + " ==========");
      String query = testQueries.get(i);

      System.out.println("ORIGINAL (flattened):");
      String flattened = flattenQuery(query);
      System.out.println(flattened);

      System.out.println("\nFORMATTED:");
      String formatted = formatQuery(flattened, "    "); // 4 spaces base indent
      System.out.println(formatted);

      System.out.println("\n");
    }
  }

  /**
   * Step 1: Flatten a multi-line query to single line
   */
  public static String flattenQuery(String query) {
    query = query.trim();
    query = query.replaceAll("\\s+", " ");
    return query;
  }

  /**
   * Step 2: Format the flattened query according to PSL style
   * @param query - flattened SQL query
   * @param baseIndent - base indentation string
   */
  public static String formatQuery(String query, String baseIndent) {
    return formatQueryRecursive(query, baseIndent, 0);
  }

  /**
   * Recursive query formatter to handle subqueries
   */
  private static String formatQueryRecursive(String query, String baseIndent, int depth) {
    String formatted = query;

    // Calculate indentation for this depth level
    String indent = baseIndent + "    ".repeat(depth);
    String colIndent = indent + "    ";
    String clauseIndent = indent;
    String whereIndent = indent + "    ";

    // RULE 1: Uppercase SQL keywords
    formatted = uppercaseKeywords(formatted);

    // RULE 2: Handle subqueries FIRST (process from inside out)
    formatted = formatSubqueries(formatted, baseIndent, depth);

    // RULE 3: SELECT - columns on separate lines
    formatted = formatSelect(formatted, colIndent);

    // RULE 4: FROM clause
    formatted = formatFrom(formatted, clauseIndent, colIndent);

    // RULE 5: JOIN clauses
    formatted = formatJoins(formatted, colIndent);

    // RULE 6: ON clause (with AND conditions)
    formatted = formatOn(formatted, colIndent);

    // RULE 7: WHERE clause
    formatted = formatWhere(formatted, clauseIndent, whereIndent);

    // RULE 8: ORDER BY clause
    formatted = formatOrderBy(formatted, clauseIndent, colIndent);

    // RULE 9: CASE statements
    formatted = formatCase(formatted, colIndent);

    return formatted;
  }

  // ========== FORMATTING RULES ==========

  private static String uppercaseKeywords(String query) {
    String[] keywords = {
        "SELECT", "FROM", "WHERE", "AND", "OR", "ORDER BY", "GROUP BY",
        "LEFT JOIN", "INNER JOIN", "RIGHT JOIN", "FULL JOIN", "OUTER JOIN", "ON", "AS",
        "DISTINCT", "CAST", "CASE", "WHEN", "THEN", "ELSE", "END",
        "IS", "NOT", "NULL", "IN", "BETWEEN", "LIKE",
        "ROW_NUMBER", "OVER", "PARTITION BY"
    };

    for (String keyword : keywords) {
      Pattern pattern = Pattern.compile("\\b" + keyword + "\\b", Pattern.CASE_INSENSITIVE);
      Matcher matcher = pattern.matcher(query);
      query = matcher.replaceAll(keyword.toUpperCase());
    }

    return query;
  }

  /**
   * Find and format subqueries (SELECT inside parentheses)
   */
  private static String formatSubqueries(String query, String baseIndent, int depth) {
    // Pattern to find ( SELECT ... ) subqueries
    StringBuilder result = new StringBuilder();
    int i = 0;

    while (i < query.length()) {
      // Look for "( SELECT" pattern
      if (i < query.length() - 8 &&
          query.charAt(i) == '(' &&
          query.substring(i + 1).trim().toUpperCase().startsWith("SELECT")) {

        // Find matching closing parenthesis
        int start = i;
        int parenDepth = 1;
        int j = i + 1;

        while (j < query.length() && parenDepth > 0) {
          if (query.charAt(j) == '(') parenDepth++;
          if (query.charAt(j) == ')') parenDepth--;
          j++;
        }

        // Extract subquery (without outer parens)
        String subquery = query.substring(start + 1, j - 1).trim();

        // Format subquery recursively with increased depth
        String formattedSubquery = formatQueryRecursive(subquery, baseIndent, depth + 1);

        // Add back with parentheses and proper formatting
        String subIndent = baseIndent + "    ".repeat(depth + 1);
        result.append("( ");
        result.append(formattedSubquery);
        result.append(" )");

        i = j;
      } else {
        result.append(query.charAt(i));
        i++;
      }
    }

    return result.toString();
  }

  private static String formatSelect(String query, String colIndent) {
    // Find SELECT portion - need to handle both main SELECT and subquery SELECT
    // Look for SELECT ... FROM, but be careful of nested SELECTs

    int selectIndex = query.toUpperCase().indexOf("SELECT ");
    if (selectIndex == -1) return query;

    // Find the FROM that matches this SELECT (same depth)
    int fromIndex = findMatchingFrom(query, selectIndex);
    if (fromIndex == -1) return query;

    String beforeSelect = query.substring(0, selectIndex);
    String columns = query.substring(selectIndex + 7, fromIndex).trim();
    String afterFrom = query.substring(fromIndex);

    // Split columns by comma, but respect parentheses
    List<String> columnList = splitByComma(columns);

    // Format: SELECT\n    col1,\n    col2,...
    String formattedColumns = "SELECT\n" + colIndent + String.join(",\n" + colIndent, columnList);

    return beforeSelect + formattedColumns + "\n" + afterFrom;
  }

  /**
   * Find the FROM clause that belongs to a SELECT at the same depth
   */
  private static int findMatchingFrom(String query, int selectIndex) {
    int parenDepth = 0;
    String upperQuery = query.toUpperCase();

    for (int i = selectIndex; i < query.length() - 4; i++) {
      char c = query.charAt(i);
      if (c == '(') parenDepth++;
      if (c == ')') parenDepth--;

      // Look for FROM at the same depth as SELECT
      if (parenDepth == 0 && upperQuery.substring(i).startsWith("FROM ")) {
        return i;
      }
    }

    return -1;
  }

  private static String formatFrom(String query, String clauseIndent, String colIndent) {
    // FROM on its own line with table on next line
    query = query.replaceAll("\\bFROM\\s+", "FROM\n" + colIndent);
    return query;
  }

  private static String formatJoins(String query, String colIndent) {
    String[] joinTypes = {"LEFT JOIN", "INNER JOIN", "RIGHT JOIN", "FULL JOIN", "OUTER JOIN"};

    for (String joinType : joinTypes) {
      query = query.replaceAll("\\s+" + joinType + "\\s+", "\n" + colIndent + joinType + " ");
    }

    return query;
  }

  private static String formatOn(String query, String colIndent) {
    // ON clause on new line with condition indented on the same line
    // Pattern: ON <condition>
    // Replace to: ON <condition> (keeping condition on same line, just indented)

    StringBuilder result = new StringBuilder();
    String upperQuery = query.toUpperCase();
    int lastIndex = 0;

    int onIndex = upperQuery.indexOf(" ON ");
    while (onIndex != -1) {
      // Add everything before ON
      result.append(query.substring(lastIndex, onIndex));

      // Find where this ON clause ends (at WHERE, another JOIN, or closing paren at depth 0)
      int conditionStart = onIndex + 4; // Skip " ON "
      int conditionEnd = findOnClauseEnd(query, conditionStart);

      String condition = query.substring(conditionStart, conditionEnd).trim();

      // Split condition by AND (at depth 0)
      List<String> conditions = splitByAnd(condition);

      // Format ON with conditions
      result.append("\n").append(colIndent).append("    ON ");
      result.append(conditions.get(0));

      for (int i = 1; i < conditions.size(); i++) {
        result.append("\n").append(colIndent).append("    AND ").append(conditions.get(i));
      }

      lastIndex = conditionEnd;
      onIndex = upperQuery.indexOf(" ON ", lastIndex);
    }

    // Add remaining query
    result.append(query.substring(lastIndex));

    return result.toString();
  }

  /**
   * Find where an ON clause ends
   */
  private static int findOnClauseEnd(String query, int startIndex) {
    String upperQuery = query.toUpperCase();
    int parenDepth = 0;

    for (int i = startIndex; i < query.length(); i++) {
      char c = query.charAt(i);

      if (c == '(') parenDepth++;
      if (c == ')') {
        if (parenDepth == 0) {
          // We've hit the closing paren of a subquery
          return i;
        }
        parenDepth--;
      }

      // Check for clause endings at depth 0
      if (parenDepth == 0) {
        String remaining = upperQuery.substring(i);
        if (remaining.startsWith("WHERE ") ||
            remaining.startsWith("LEFT JOIN ") ||
            remaining.startsWith("INNER JOIN ") ||
            remaining.startsWith("RIGHT JOIN ") ||
            remaining.startsWith("FULL JOIN ") ||
            remaining.startsWith("ORDER BY ") ||
            remaining.startsWith("GROUP BY ")) {
          return i;
        }
      }
    }

    return query.length();
  }

  /**
   * Split a string by AND, but respect parentheses depth
   */
  private static List<String> splitByAnd(String text) {
    List<String> parts = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    int parenDepth = 0;
    String upperText = text.toUpperCase();

    int i = 0;
    while (i < text.length()) {
      char c = text.charAt(i);

      if (c == '(') {
        parenDepth++;
      } else if (c == ')') {
        parenDepth--;
      }

      // Check for AND at depth 0
      if (parenDepth == 0 && i < text.length() - 4) {
        String ahead = upperText.substring(i, Math.min(i + 5, text.length()));
        if (ahead.startsWith(" AND ")) {
          parts.add(current.toString().trim());
          current = new StringBuilder();
          i += 5; // Skip " AND "
          continue;
        }
      }

      current.append(c);
      i++;
    }

    if (current.length() > 0) {
      parts.add(current.toString().trim());
    }

    return parts;
  }

  private static String formatOnConditions(String query, String colIndent) {
    // Find ON clauses and format their AND conditions
    StringBuilder result = new StringBuilder();
    String[] lines = query.split("\n");
    boolean inOnClause = false;

    for (String line : lines) {
      String trimmed = line.trim();

      if (trimmed.startsWith("ON")) {
        inOnClause = true;
      } else if (trimmed.startsWith("WHERE") || trimmed.startsWith("LEFT JOIN") ||
          trimmed.startsWith("INNER JOIN") || trimmed.startsWith("ORDER BY")) {
        inOnClause = false;
      }

      if (inOnClause && !trimmed.startsWith("ON")) {
        // Split by AND
        String formatted = trimmed.replaceAll("\\bAND\\b", "\n" + colIndent + "    AND");
        result.append(formatted).append("\n");
      } else {
        result.append(line).append("\n");
      }
    }

    return result.toString().trim();
  }

  private static String formatWhere(String query, String clauseIndent, String whereIndent) {
    // WHERE on its own line
    query = query.replaceAll("\\bWHERE\\s+", "\nWHERE\n" + whereIndent);

    // AND conditions in WHERE clause
    // Find WHERE section and format ANDs within it
    int whereIndex = query.toUpperCase().indexOf("\nWHERE\n");
    if (whereIndex != -1) {
      // Find end of WHERE clause (ORDER BY, GROUP BY, or end)
      int endIndex = query.length();
      String[] endMarkers = {"ORDER BY", "GROUP BY", "LIMIT"};
      for (String marker : endMarkers) {
        int idx = query.toUpperCase().indexOf(marker, whereIndex);
        if (idx != -1 && idx < endIndex) {
          endIndex = idx;
        }
      }

      String beforeWhere = query.substring(0, whereIndex);
      String whereClause = query.substring(whereIndex, endIndex);
      String afterWhere = query.substring(endIndex);

      // Format ANDs in WHERE clause
      whereClause = whereClause.replaceAll("\\s+AND\\s+", "\n" + whereIndent + "AND ");

      query = beforeWhere + whereClause + afterWhere;
    }

    return query;
  }

  private static String formatOrderBy(String query, String clauseIndent, String colIndent) {
    query = query.replaceAll("\\bORDER BY\\s+", "\nORDER BY\n" + colIndent);
    return query;
  }

  private static String formatCase(String query, String colIndent) {
    // CASE on new line
    query = query.replaceAll("\\bCASE\\b", "\n" + colIndent + "        CASE");

    // WHEN on new line
    query = query.replaceAll("\\bWHEN\\b", "\n" + colIndent + "            WHEN");

    // THEN stays on same line as WHEN
    // ELSE on new line
    query = query.replaceAll("\\bELSE\\b", "\n" + colIndent + "            ELSE");

    // END on new line
    query = query.replaceAll("\\bEND\\b", "\n" + colIndent + "        END");

    return query;
  }

  // ========== HELPER METHODS ==========

  /**
   * Split a string by comma, but respect parentheses depth
   */
  private static List<String> splitByComma(String text) {
    List<String> parts = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    int parenDepth = 0;

    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);

      if (c == '(') {
        parenDepth++;
      } else if (c == ')') {
        parenDepth--;
      } else if (c == ',' && parenDepth == 0) {
        parts.add(current.toString().trim());
        current = new StringBuilder();
        continue;
      }

      current.append(c);
    }

    if (current.length() > 0) {
      parts.add(current.toString().trim());
    }

    return parts;
  }
}