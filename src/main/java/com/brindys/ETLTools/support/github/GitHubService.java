package com.brindys.ETLTools.support.github;

import com.brindys.ETLTools.config.GitHubConfig;
import com.brindys.ETLTools.support.github.dto.CommitRequest;
import com.brindys.ETLTools.support.github.dto.CommitResponse;
import com.brindys.ETLTools.support.github.dto.MappingHistory;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.github.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;  // ADD THIS
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class GitHubService {

  @Autowired
  private GitHub gitHub;

  @Autowired
  private GitHubConfig config;

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Value("${github.token}")
  private String githubToken;

  /**
   * Commit a visit type mapping to GitHub
   */
  public CommitResponse commitMapping(CommitRequest request) {
    try {
      log.info("Committing mapping to GitHub: {}", request.getMessage());

      GHRepository repo = gitHub.getRepository(config.getFullRepoPath());

      // Convert mapping to JSON
      String jsonContent = objectMapper.writerWithDefaultPrettyPrinter()
          .writeValueAsString(request.getMapping());

      // Create filename with timestamp
      String timestamp = LocalDateTime.now()
          .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
      String filename = "mappings/visit_type_mapping_" + timestamp + ".json";

      // Build commit message
      String commitMessage = request.getMessage();
      if (request.getAuthor() != null && !request.getAuthor().isEmpty()) {
        commitMessage += " (by " + request.getAuthor() + ")";
      }

      // Commit to GitHub
      GHContentBuilder contentBuilder = repo.createContent()
          .content(jsonContent)
          .path(filename)
          .branch(config.getBranch())
          .message(commitMessage);

      GHContentUpdateResponse updateResponse = contentBuilder.commit();
      GHContent content = updateResponse.getContent();

      log.info("Successfully committed mapping: {}", filename);

      CommitResponse response = new CommitResponse();
      response.setSuccess(true);
      response.setMessage("Successfully committed to GitHub");
      response.setCommitSha(updateResponse.getCommit().getSHA1());
      response.setCommitUrl(content.getHtmlUrl());
      response.setTimestamp(LocalDateTime.now());

      return response;

    } catch (IOException e) {
      log.error("Error committing to GitHub", e);

      CommitResponse response = new CommitResponse();
      response.setSuccess(false);
      response.setMessage("Failed to commit: " + e.getMessage());
      response.setTimestamp(LocalDateTime.now());

      return response;
    }
  }

  /**
   * Get the history of all committed mappings
   */
  public List<MappingHistory> getMappingHistory() throws IOException {
    log.info("Fetching mapping history from GitHub");

    GHRepository repo = gitHub.getRepository(config.getFullRepoPath());

    // Get all files in the mappings directory
    List<GHContent> contents = repo.getDirectoryContent("mappings", config.getBranch());

    // Convert to MappingHistory objects and sort by date (newest first)
    List<MappingHistory> history = contents.stream()
        .filter(content -> content.getName().endsWith(".json"))
        .map(content -> {
          try {
            // Get commit info for this file
            PagedIterable<GHCommit> commits = repo.queryCommits()
                .path(content.getPath())
                .pageSize(1)
                .list();

            GHCommit lastCommit = commits.iterator().next();

            MappingHistory history1 = new MappingHistory();
            history1.setFilename(content.getName());
            history1.setCommitMessage(lastCommit.getCommitShortInfo().getMessage());
            history1.setAuthor(lastCommit.getCommitShortInfo().getAuthor().getName());
            history1.setDate(LocalDateTime.ofInstant(
                lastCommit.getCommitDate().toInstant(),
                ZoneId.systemDefault()));
            history1.setDownloadUrl(content.getDownloadUrl());

            return history1;
          } catch (IOException e) {
            log.error("Error getting commit info for: {}", content.getName(), e);
            return null;
          }
        })
        .filter(h -> h != null)
        .sorted((h1, h2) -> h2.getDate().compareTo(h1.getDate())) // Newest first
        .collect(Collectors.toList());

    log.info("Found {} mapping files", history.size());
    return history;
  }

  /**
   * Get the repository info (useful for testing connection)
   */
  public String getRepositoryInfo() throws IOException {
    GHRepository repo = gitHub.getRepository(config.getFullRepoPath());
    return String.format("Connected to: %s (default branch: %s)",
        repo.getFullName(),
        repo.getDefaultBranch());
  }

  /**
   * Load a specific mapping from GitHub
   */
  /**
   * Load a specific mapping from GitHub
   */
  /**
   * Load a specific mapping from GitHub
   */
  public Map<String, String> loadMapping(String filename) throws IOException {
    log.info("Loading mapping from GitHub: {}", filename);

    GHRepository repo = gitHub.getRepository(config.getFullRepoPath());

    // getContent only takes the path, it uses the default branch automatically
    // or you need to use getFileContent with a ref
    GHContent content = repo.getFileContent("mappings/" + filename, config.getBranch());

    String jsonContent = new String(content.read().readAllBytes());

    @SuppressWarnings("unchecked")
    Map<String, String> mapping = objectMapper.readValue(jsonContent, Map.class);

    return mapping;
  }
}