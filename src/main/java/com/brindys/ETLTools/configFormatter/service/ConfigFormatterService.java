package com.brindys.ETLTools.configFormatter.service;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class ConfigFormatterService {

  /**
   * Main entry point - formats an entire PSL config
   */
  public String formatConfig(String config) {
    // Step 1: Extract and protect VARIABLE lines (before any processing)
    List<ProtectedBlock> variableBlocks = extractVariableBlocks(config);
    String text = replaceWithPlaceholders(config, variableBlocks, "VARIABLE_PLACEHOLDER_");

    // Step 2: Extract and protect hierarchy section (everything after #---hier---)
    List<ProtectedBlock> hierBlocks = extractHierarchyBlocks(text);
    text = replaceWithPlaceholders(text, hierBlocks, "HIER_PLACEHOLDER_");

    // Step 3: Extract and protect SQL queries (before brace normalization)
    List<ProtectedBlock> sqlBlocks = extractSqlBlocks(text);
    text = replaceWithPlaceholders(text, sqlBlocks, "SQL_PLACEHOLDER_");

    // Step 4: Now safe to normalize braces and other formatting
    text = normalizeLineSpacing(text);
    text = normalizeEqualsSpacing(text);
    text = normalizeBraceSpacing(text);
    text = normalizeClosingBraces(text);

    // Step 5: Standardize indentation
    text = standardizeIndentation(text);

    // Step 6: Restore and format SQL blocks
    text = restoreAndFormatSqlBlocks(text, sqlBlocks);

    // Step 7: Restore VARIABLE blocks (unchanged)
    text = restoreBlocks(text, variableBlocks, "VARIABLE_PLACEHOLDER_");

    // Step 8: Restore hierarchy blocks (unchanged)
    text = restoreBlocks(text, hierBlocks, "HIER_PLACEHOLDER_");

    // Step 9: Clean up
    text = text.replaceAll("\n{3,}", "\n\n");
    text = removeBlankLinesInBlocks(text);

    return text;
  }

  // ========== BLOCK PROTECTION ==========

  private static class ProtectedBlock {
    int index;
    String original;
    String formatted;

    ProtectedBlock(int index, String original) {
      this.index = index;
      this.original = original;
      this.formatted = original;
    }
  }

  private List<ProtectedBlock> extractVariableBlocks(String config) {
    List<ProtectedBlock> blocks = new ArrayList<>();
    Pattern pattern = Pattern.compile(
        "VARIABLE\\s+\\w+\\s*\\{[^}]*\\}",
        Pattern.CASE_INSENSITIVE
    );

    Matcher matcher = pattern.matcher(config);
    int index = 0;
    while (matcher.find()) {
      blocks.add(new ProtectedBlock(index++, matcher.group()));
    }
    return blocks;
  }

  /**
   * Extract everything from #---hier--- line to end of file
   * This section should not be formatted
   */
  private List<ProtectedBlock> extractHierarchyBlocks(String config) {
    List<ProtectedBlock> blocks = new ArrayList<>();
    Pattern pattern = Pattern.compile(
        "(#-+\\s*hier.*)",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    Matcher matcher = pattern.matcher(config);
    int index = 0;
    while (matcher.find()) {
      blocks.add(new ProtectedBlock(index++, matcher.group()));
    }
    return blocks;
  }

  private List<ProtectedBlock> extractSqlBlocks(String config) {
    List<ProtectedBlock> blocks = new ArrayList<>();
    Pattern pattern = Pattern.compile(
        "(QUERY\\s*=\\s*)([\\s\\S]*?)(?=\n\\s*\\})",
        Pattern.CASE_INSENSITIVE
    );

    Matcher matcher = pattern.matcher(config);
    int index = 0;
    while (matcher.find()) {
      blocks.add(new ProtectedBlock(index++, matcher.group()));
    }
    return blocks;
  }

  private String replaceWithPlaceholders(String text, List<ProtectedBlock> blocks, String prefix) {
    for (int i = blocks.size() - 1; i >= 0; i--) {
      ProtectedBlock block = blocks.get(i);
      text = text.replace(block.original, prefix + block.index);
    }
    return text;
  }

  private String restoreBlocks(String text, List<ProtectedBlock> blocks, String prefix) {
    for (ProtectedBlock block : blocks) {
      text = text.replace(prefix + block.index, block.formatted);
    }
    return text;
  }

  private String restoreAndFormatSqlBlocks(String text, List<ProtectedBlock> blocks) {
    for (ProtectedBlock block : blocks) {
      String original = block.original;
      Pattern queryPattern = Pattern.compile("(QUERY\\s*=\\s*)(.*)", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
      Matcher matcher = queryPattern.matcher(original);

      if (matcher.find()) {
        String prefix = matcher.group(1);
        String sql = matcher.group(2);

        String flattened = flattenQuery(sql);
        String formatted = formatQuery(flattened, "        "); // 8 spaces base indent

        block.formatted = prefix + formatted;
      }

      text = text.replace("SQL_PLACEHOLDER_" + block.index, block.formatted);
    }
    return text;
  }

  // ========== NORMALIZATION METHODS ==========

  private String normalizeLineSpacing(String text) {
    String[] lines = text.split("\n");
    StringBuilder result = new StringBuilder();
    for (String line : lines) {
      if (line.contains("VARIABLE_PLACEHOLDER_") || line.contains("SQL_PLACEHOLDER_") || line.contains("HIER_PLACEHOLDER_")) {
        result.append(line).append("\n");
      } else {
        result.append(line.replaceAll("\\s{2,}", " ")).append("\n");
      }
    }
    return result.toString().trim();
  }

  private String normalizeEqualsSpacing(String text) {
    String[] lines = text.split("\n");
    StringBuilder result = new StringBuilder();
    for (String line : lines) {
      if (line.contains("VARIABLE_PLACEHOLDER_") || line.contains("SQL_PLACEHOLDER_") || line.contains("HIER_PLACEHOLDER_")) {
        result.append(line).append("\n");
      } else {
        result.append(line.replaceAll("(\\w+)\\s*=\\s*", "$1 = ")).append("\n");
      }
    }
    return result.toString().trim();
  }

  private String normalizeBraceSpacing(String text) {
    String[] lines = text.split("\n");
    StringBuilder result = new StringBuilder();
    for (String line : lines) {
      if (line.contains("VARIABLE_PLACEHOLDER_") || line.contains("SQL_PLACEHOLDER_") || line.contains("HIER_PLACEHOLDER_")) {
        result.append(line).append("\n");
      } else {
        result.append(line.replaceAll("\\s*\\{\\s*$", " {")).append("\n");
      }
    }
    return result.toString().trim();
  }

  private String normalizeClosingBraces(String text) {
    return text.replaceAll("([^\\s\\n])\\s*\\}\\s*$", "$1\n}");
  }

  private String standardizeIndentation(String text) {
    String[] lines = text.split("\n");
    StringBuilder result = new StringBuilder();
    int indentLevel = 0;

    for (String line : lines) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) {
        result.append("\n");
        continue;
      }

      if (trimmed.startsWith("VARIABLE_PLACEHOLDER_") || trimmed.startsWith("SQL_PLACEHOLDER_") || trimmed.startsWith("HIER_PLACEHOLDER_")) {
        result.append("    ".repeat(indentLevel)).append(trimmed).append("\n");
        continue;
      }

      if (trimmed.toUpperCase().startsWith("FEATURE ")) {
        result.append(trimmed).append("\n");
        continue;
      }

      if (trimmed.equals("}")) {
        indentLevel = Math.max(0, indentLevel - 1);
      }

      result.append("    ".repeat(indentLevel)).append(trimmed).append("\n");

      if (trimmed.endsWith("{")) {
        indentLevel++;
      }
    }

    return result.toString().trim();
  }

  private String removeBlankLinesInBlocks(String text) {
    StringBuilder result = new StringBuilder();
    String[] lines = text.split("\n");
    boolean lastWasBlank = false;
    int braceDepth = 0;

    for (String line : lines) {
      String trimmed = line.trim();

      for (char c : trimmed.toCharArray()) {
        if (c == '{') braceDepth++;
        if (c == '}') braceDepth--;
      }

      boolean isBlank = trimmed.isEmpty();

      if (braceDepth > 0 && isBlank && lastWasBlank) {
        continue;
      }

      result.append(line).append("\n");
      lastWasBlank = isBlank;
    }

    return result.toString().trim();
  }

  // ========== SQL FORMATTING ==========

  public String flattenQuery(String query) {
    query = query.trim();
    query = query.replaceAll("\\s+", " ");
    return query;
  }

  public String formatQuery(String query, String baseIndent) {
    return formatQueryWithDepth(query, baseIndent, 0);
  }

  private String formatQueryWithDepth(String query, String baseIndent, int depth) {
    // Indentation levels (at depth 0 with baseIndent of 8 spaces):
    // clauseIndent = 12 spaces - for FROM, JOIN, WHERE
    // colIndent    = 16 spaces - for columns, tables, WHERE conditions
    // onIndent     = 16 spaces - for ON (same as colIndent)
    // onCondIndent = 20 spaces - for ON conditions
    String clauseIndent = baseIndent + "    " + "    ".repeat(depth);    // base + 4 + depth
    String colIndent = baseIndent + "        " + "    ".repeat(depth);   // base + 8 + depth
    String onCondIndent = baseIndent + "            " + "    ".repeat(depth); // base + 12 + depth

    // Step 1: Uppercase keywords
    String formatted = uppercaseKeywords(query);

    // Step 2: Handle subqueries first (recursive)
    formatted = formatSubqueries(formatted, baseIndent, depth);

    // Step 3: Format SELECT columns
    formatted = formatSelect(formatted, colIndent);

    // Step 4: Format FROM
    formatted = formatFrom(formatted, clauseIndent, colIndent);

    // Step 5: Format JOINs
    formatted = formatJoins(formatted, clauseIndent, colIndent);

    // Step 6: Format ON
    formatted = formatOn(formatted, colIndent, onCondIndent);

    // Step 7: Format WHERE
    formatted = formatWhere(formatted, clauseIndent, colIndent);

    // Step 8: Format ORDER BY
    formatted = formatOrderBy(formatted, clauseIndent, colIndent);

    // Step 9: Format CASE statements
    formatted = formatCase(formatted, colIndent);

    // Step 10: Format OVER clauses (PARTITION BY, ORDER BY inside OVER)
    formatted = formatOver(formatted, colIndent);

    return formatted;
  }

  private String uppercaseKeywords(String query) {
    String[] keywords = {
        "SELECT", "FROM", "WHERE", "AND", "OR", "ORDER BY", "GROUP BY",
        "LEFT JOIN", "INNER JOIN", "RIGHT JOIN", "FULL JOIN", "ON", "AS",
        "DISTINCT", "CAST", "CASE", "WHEN", "THEN", "ELSE", "END",
        "IS", "NOT", "NULL", "IN", "BETWEEN", "LIKE",
        "ROW_NUMBER", "OVER", "PARTITION BY", "COALESCE"
    };

    for (String keyword : keywords) {
      Pattern pattern = Pattern.compile("\\b" + Pattern.quote(keyword) + "\\b", Pattern.CASE_INSENSITIVE);
      Matcher matcher = pattern.matcher(query);
      query = matcher.replaceAll(keyword.toUpperCase());
    }

    return query;
  }

  private String formatSubqueries(String query, String baseIndent, int depth) {
    StringBuilder result = new StringBuilder();
    int i = 0;

    while (i < query.length()) {
      // Look for ( SELECT pattern
      if (query.charAt(i) == '(' && i + 1 < query.length()) {
        String ahead = query.substring(i + 1).trim();
        if (ahead.toUpperCase().startsWith("SELECT")) {
          // Find matching closing paren
          int start = i;
          int parenDepth = 1;
          int j = i + 1;

          while (j < query.length() && parenDepth > 0) {
            if (query.charAt(j) == '(') parenDepth++;
            if (query.charAt(j) == ')') parenDepth--;
            j++;
          }

          String subquery = query.substring(start + 1, j - 1).trim();
          String formattedSubquery = formatQueryWithDepth(subquery, baseIndent, depth + 1);

          // Indent for the SELECT keyword of the subquery (colIndent at current depth)
          String selectIndent = baseIndent + "        " + "    ".repeat(depth);
          // Closing paren at clauseIndent level for this depth
          String closingIndent = baseIndent + "    " + "    ".repeat(depth);

          result.append("(\n");
          result.append(selectIndent);  // Add indentation before SELECT
          result.append(formattedSubquery);
          result.append("\n").append(closingIndent).append(")");

          i = j;
          continue;
        }
      }

      result.append(query.charAt(i));
      i++;
    }

    return result.toString();
  }

  private String formatSelect(String query, String colIndent) {
    // Find SELECT ... FROM at the top level (not inside parens)
    int selectIndex = findKeywordAtTopLevel(query, "SELECT ");
    if (selectIndex == -1) return query;

    int fromIndex = findKeywordAtTopLevel(query.substring(selectIndex), "FROM ");
    if (fromIndex == -1) return query;
    fromIndex += selectIndex;

    String beforeSelect = query.substring(0, selectIndex);
    String columns = query.substring(selectIndex + 7, fromIndex).trim();
    String afterFrom = query.substring(fromIndex);

    List<String> columnList = splitByComma(columns);
    String formattedColumns = "SELECT\n" + colIndent + String.join(",\n" + colIndent, columnList);

    return beforeSelect + formattedColumns + "\n" + afterFrom;
  }

  private String formatFrom(String query, String clauseIndent, String colIndent) {
    // FROM keyword on its own line with clauseIndent, table on next line with colIndent
    query = query.replaceAll("\\bFROM\\b(?!\\s*\n)", "\n" + clauseIndent + "FROM\n" + colIndent);
    return query;
  }

  private String formatJoins(String query, String clauseIndent, String colIndent) {
    String[] joinTypes = {"LEFT JOIN", "INNER JOIN", "RIGHT JOIN", "FULL JOIN"};

    for (String joinType : joinTypes) {
      // Handle JOIN with subquery: LEFT JOIN (
      query = query.replaceAll("\\s*" + joinType + "\\s*\\(", "\n" + clauseIndent + joinType + " (");
      // Handle JOIN with table: LEFT JOIN tablename
      query = query.replaceAll("\\s*" + joinType + "\\s+(?!\\()", "\n" + clauseIndent + joinType + " ");
    }

    return query;
  }

  private String formatOn(String query, String onIndent, String onCondIndent) {
    // ON on its own line at onIndent, conditions on next line at onCondIndent
    query = query.replaceAll("\\s+ON\\s+", "\n" + onIndent + "ON\n" + onCondIndent);
    return query;
  }

  private String formatWhere(String query, String clauseIndent, String colIndent) {
    // WHERE on its own line, conditions on next line
    query = query.replaceAll("\\s+WHERE\\s+", "\n" + clauseIndent + "WHERE\n" + colIndent);

    // AND at top level gets new line
    StringBuilder result = new StringBuilder();
    int parenDepth = 0;
    int i = 0;
    String upperQuery = query.toUpperCase();

    while (i < query.length()) {
      char c = query.charAt(i);
      if (c == '(') parenDepth++;
      if (c == ')') parenDepth--;

      // Check for AND at top level (parenDepth == 0)
      if (parenDepth == 0 && i + 5 <= query.length()) {
        String ahead = upperQuery.substring(i, Math.min(i + 5, query.length()));
        if (ahead.equals(" AND ")) {
          result.append("\n").append(colIndent).append("AND ");
          i += 5;
          continue;
        }
      }

      result.append(c);
      i++;
    }

    return result.toString();
  }

  private String formatOrderBy(String query, String clauseIndent, String colIndent) {
    // Only format ORDER BY that's NOT inside OVER()
    // This is tricky - for now, just handle top-level ORDER BY
    // The OVER() ORDER BY will be handled by formatOver
    return query;
  }

  private String formatCase(String query, String colIndent) {
    String caseIndent = colIndent + "    ";
    String whenIndent = colIndent + "        ";

    // CASE on new line
    query = query.replaceAll("\\bCASE\\b", "\n" + caseIndent + "CASE");
    // WHEN on new line
    query = query.replaceAll("\\s+WHEN\\b", "\n" + whenIndent + "WHEN");
    // ELSE on new line
    query = query.replaceAll("\\s+ELSE\\b", "\n" + whenIndent + "ELSE");
    // END on new line (but not END, which is end of CASE followed by comma)
    query = query.replaceAll("\\s+END\\b", "\n" + caseIndent + "END");

    return query;
  }

  private String formatOver(String query, String colIndent) {
    String overIndent = colIndent + "    ";
    String partitionIndent = colIndent + "        ";

    // OVER ( on new line
    query = query.replaceAll("\\bOVER\\s*\\(", "\n" + overIndent + "OVER (\n" + partitionIndent);
    // PARTITION BY
    query = query.replaceAll("\\bPARTITION BY\\b", "PARTITION BY");
    // ORDER BY inside OVER - add newline
    query = query.replaceAll("\\s+ORDER BY\\s+", "\n" + partitionIndent + "ORDER BY\n" + partitionIndent + "    ");

    return query;
  }

  private int findKeywordAtTopLevel(String query, String keyword) {
    int parenDepth = 0;
    String upperQuery = query.toUpperCase();
    String upperKeyword = keyword.toUpperCase();

    for (int i = 0; i <= query.length() - keyword.length(); i++) {
      char c = query.charAt(i);
      if (c == '(') parenDepth++;
      if (c == ')') parenDepth--;

      if (parenDepth == 0 && upperQuery.substring(i).startsWith(upperKeyword)) {
        return i;
      }
    }
    return -1;
  }

  private List<String> splitByComma(String text) {
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