package com.brindys.ETLTools.config;

import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class GitHubConfig {

  @Value("${github.token}")
  private String githubToken;

  @Value("${github.repo.owner:your-org}")
  private String repoOwner;

  @Value("${github.repo.name:visit-type-mappings}")
  private String repoName;

  @Value("${github.branch:main}")
  private String branch;

  @Bean
  public GitHub gitHub() throws IOException {
    return new GitHubBuilder()
        .withOAuthToken(githubToken)
        .build();
  }

  // Getters for other services to use
  public String getRepoOwner() {
    return repoOwner;
  }

  public String getRepoName() {
    return repoName;
  }

  public String getBranch() {
    return branch;
  }

  public String getFullRepoPath() {
    return repoOwner + "/" + repoName;
  }
}