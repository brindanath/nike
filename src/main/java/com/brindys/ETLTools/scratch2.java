package com.brindys.ETLTools;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class scratch2 {

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
                            CAST(person_id AS string
                        ) AS person_id, provider_id, code, CAST(event_date AS DATE
                        ) AS procedure_date
                        FROM
                            $DATA_SCHEMA.procedure p
                            INNER JOIN $DATA_SCHEMA.atropos_concepts c
                            ON
                                c.concept_code = p.code
                        WHERE
                            LEFT(person_id, 1) = '1'
                            AND c.vocabulary_id IN ('CPT4', 'HCPCS')
                            AND c.concept_class_id != 'CPT4 Hierarchy' $PERSON_AND_SUB
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
      String formatted = formatQuery(flattened, "        "); // 8 spaces base indent
      System.out.println(formatted);

      System.out.println("\n");
    }
  }

  /**
   * Step 1: Flatten a multi-line query to single line
   */
  public static String flattenQuery(String query) {
    // Remove leading/trailing whitespace
    query = query.trim();

    // Replace all whitespace (including newlines) with single space
    query = query.replaceAll("\\s+", " ");

    return query;
  }

  /**
   * Step 2: Format the flattened query according to PSL style
   * @param query - flattened SQL query
   * @param baseIndent - base indentation string (e.g., "        " for 8 spaces)
   */
  public static String formatQuery(String query, String baseIndent) {
    String formatted = query;

    // Calculate indentation levels
    String colIndent = baseIndent + "            "; // 12 additional spaces for columns
    String clauseIndent = baseIndent + "        ";   // 8 additional spaces for clauses
    String whereIndent = baseIndent + "            "; // 12 additional spaces for WHERE conditions

    // RULE 1: Uppercase SQL keywords
    formatted = uppercaseKeywords(formatted);

    // RULE 2: SELECT - columns on separate lines
    formatted = formatSelect(formatted, colIndent);

    // RULE 3: FROM clause
    formatted = formatFrom(formatted, clauseIndent, colIndent);

    // RULE 4: JOIN clauses
    formatted = formatJoins(formatted, colIndent);

    // RULE 5: WHERE clause
    formatted = formatWhere(formatted, clauseIndent, whereIndent);

    // Add more rules as needed...

    return formatted;
  }

  // ========== FORMATTING RULES ==========

  private static String uppercaseKeywords(String query) {
    String[] keywords = {
        "SELECT", "FROM", "WHERE", "AND", "OR", "ORDER BY", "GROUP BY",
        "LEFT JOIN", "INNER JOIN", "RIGHT JOIN", "FULL JOIN", "ON", "AS",
        "DISTINCT", "CAST", "CASE", "WHEN", "THEN", "ELSE", "END",
        "IS", "NOT", "NULL", "IN", "BETWEEN", "LIKE",
        "ROW_NUMBER", "OVER", "PARTITION BY"
    };

    for (String keyword : keywords) {
      // Use word boundaries to avoid partial matches
      Pattern pattern = Pattern.compile("\\b" + keyword + "\\b", Pattern.CASE_INSENSITIVE);
      Matcher matcher = pattern.matcher(query);
      query = matcher.replaceAll(keyword.toUpperCase());
    }

    return query;
  }

  private static String formatSelect(String query, String colIndent) {
    // Find SELECT portion (up to FROM)
    Pattern selectPattern = Pattern.compile("SELECT\\s+(.*?)\\s+FROM", Pattern.CASE_INSENSITIVE);
    Matcher matcher = selectPattern.matcher(query);

    if (matcher.find()) {
      String columns = matcher.group(1);

      // Split columns by comma, but respect parentheses
      List<String> columnList = splitByComma(columns);

      // Format: SELECT\n    col1,\n    col2,...
      String formattedColumns = "SELECT\n" + colIndent + String.join(",\n" + colIndent, columnList);

      query = matcher.replaceFirst(formattedColumns + "\nFROM");
    }

    return query;
  }

  private static String formatFrom(String query, String clauseIndent, String colIndent) {
    // FROM on its own line with proper indent, table on next line
    query = query.replaceAll("\\bFROM\\s+", "\n" + clauseIndent + "FROM\n" + colIndent);
    return query;
  }

  private static String formatJoins(String query, String colIndent) {
    // JOIN types on new lines
    String[] joinTypes = {"LEFT JOIN", "INNER JOIN", "RIGHT JOIN", "FULL JOIN"};

    for (String joinType : joinTypes) {
      query = query.replaceAll("\\s+" + joinType + "\\s+", "\n" + colIndent + joinType + " ");
    }

    // ON clause
    query = query.replaceAll("\\s+ON\\s+", "\n" + colIndent + "ON\n" + colIndent + "    ");

    return query;
  }

  private static String formatWhere(String query, String clauseIndent, String whereIndent) {
    // WHERE on its own line
    query = query.replaceAll("\\bWHERE\\s+", "\n" + clauseIndent + "WHERE\n" + whereIndent);

    // AND conditions on separate lines (but not AND inside parentheses)
    // This is a simplified version - may need refinement
    query = query.replaceAll("\\s+AND\\s+(?![^()]*\\))", "\n" + whereIndent + "AND ");

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