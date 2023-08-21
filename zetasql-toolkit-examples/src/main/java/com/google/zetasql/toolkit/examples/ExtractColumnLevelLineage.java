/*
 * Copyright 2023 Google LLC All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.zetasql.toolkit.examples;

import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import com.google.zetasql.toolkit.AnalyzedStatement;
import com.google.zetasql.toolkit.ZetaSQLToolkitAnalyzer;
import com.google.zetasql.toolkit.catalog.bigquery.BigQueryCatalog;
import com.google.zetasql.toolkit.options.BigQueryLanguageOptions;
import com.google.zetasql.toolkit.tools.lineage.ColumnLineageExtractor;
import com.google.zetasql.toolkit.tools.lineage.ColumnEntity;
import com.google.zetasql.toolkit.tools.lineage.ColumnLineage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ExtractColumnLevelLineage {
  private static void outputLineage(String query, Set<ColumnLineage> lineageEntries) {
    System.out.println("\nQuery:");
    System.out.println(query);
    System.out.println("\nLineage:");
    
    File file = new File("columns.txt");

    try {
      FileWriter fw = new FileWriter(file, false);
      fw.write("");
      fw.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    lineageEntries.forEach(lineage -> {
      System.out.printf("%s.%s\n", lineage.target.table, lineage.target.name);
      
      // writing column to columns file
      try {
        FileWriter fw = new FileWriter(file, true);
        fw.write(lineage.target.name + "\n");
        fw.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
      for (ColumnEntity parent : lineage.parents) {
        System.out.printf("\t\t<- %s.%s\n", parent.table, parent.name);
        /* 
        if (parent.table == "$union_all") {
            
        }
        */
        
      }
    });
    System.out.println();
    System.out.println();
  }

  private static void lineageForCreateTableAsSelectStatement(
      BigQueryCatalog catalog, ZetaSQLToolkitAnalyzer analyzer) {
    String query = 
      "CREATE TABLE `project.dataset.table` AS\n"
        + "SELECT\n"
        + "    concatted AS column_alias\n"
        + "FROM\n"
        + "    (\n"
        + "        SELECT \n"
        + "            UPPER(CONCAT(title, comment)) AS concatted\n"
        + "        FROM `bigquery-public-data`.samples.wikipedia\n"
        + "    )\n"
        + "GROUP BY 1;";

    Iterator<AnalyzedStatement> statementIterator = analyzer.analyzeStatements(query, catalog);
    ResolvedStatement statement = statementIterator.next().getResolvedStatement().get();

    Set<ColumnLineage> lineageEntries = ColumnLineageExtractor.extractColumnLevelLineage(statement);

    System.out.println("Extracted column lineage from CREATE TABLE AS SELECT");
    outputLineage(query, lineageEntries);
  }

  private static void lineageForInsertStatement(
      BigQueryCatalog catalog, ZetaSQLToolkitAnalyzer analyzer) {
    String query = "INSERT INTO `bigquery-public-data.samples.wikipedia`(title, comment)\n"
        + "SELECT\n"
        + "    LOWER(upper_corpus) AS titleaaaaaa,\n"
        + "    UPPER(lower_word) AS comment\n"
        + "FROM (\n"
        + "    SELECT\n"
        + "      UPPER(corpus) AS upper_corpus,\n"
        + "      LOWER(word) AS lower_word\n"
        + "    FROM `bigquery-public-data.samples.shakespeare`\n"
        + "    WHERE word_count > 10\n"
        + "    );";

    Iterator<AnalyzedStatement> statementIterator = analyzer.analyzeStatements(query, catalog);

    ResolvedStatement statement = statementIterator.next().getResolvedStatement().get();

    Set<ColumnLineage> lineageEntries = ColumnLineageExtractor.extractColumnLevelLineage(statement);

    System.out.println("Extracted column lineage from INSERT");
    outputLineage(query, lineageEntries);
  }

  private static void lineageForUpdateStatement(
      BigQueryCatalog catalog, ZetaSQLToolkitAnalyzer analyzer) {
    String query = "UPDATE `bigquery-public-data.samples.wikipedia` W\n"
        + "    SET title = S.corpus, comment = S.word\n"
        + "FROM (SELECT corpus, UPPER(word) AS word FROM `bigquery-public-data.samples.shakespeare`) S\n"
        + "WHERE W.title = S.corpus;";

    Iterator<AnalyzedStatement> statementIterator = analyzer.analyzeStatements(query, catalog);

    ResolvedStatement statement = statementIterator.next().getResolvedStatement().get();

    Set<ColumnLineage> lineageEntries = ColumnLineageExtractor.extractColumnLevelLineage(statement);

    System.out.println("Extracted column lineage from UPDATE");
    outputLineage(query, lineageEntries);
  }

  private static void lineageForMergeStatement(
      BigQueryCatalog catalog, ZetaSQLToolkitAnalyzer analyzer) {
    String query = "MERGE `bigquery-public-data.samples.wikipedia` W\n"
        + "USING (SELECT corpus, UPPER(word) AS word FROM `bigquery-public-data.samples.shakespeare`) S\n"
        + "ON W.title = S.corpus\n"
        + "WHEN MATCHED THEN\n"
        + "    UPDATE SET comment = S.word\n"
        + "WHEN NOT MATCHED THEN\n"
        + "    INSERT(title) VALUES (UPPER(corpus));";

    Iterator<AnalyzedStatement> statementIterator = analyzer.analyzeStatements(query, catalog);

    ResolvedStatement statement = statementIterator.next().getResolvedStatement().get();

    Set<ColumnLineage> lineageEntries = ColumnLineageExtractor.extractColumnLevelLineage(statement);

    System.out.println("Extracted column lineage from MERGE");
    outputLineage(query, lineageEntries);
  }

  private static void lineageForCustomStatement(BigQueryCatalog catalog, ZetaSQLToolkitAnalyzer analyzer, String query) {
    Iterator<AnalyzedStatement> statementIterator = analyzer.analyzeStatements(query, catalog);
    ResolvedStatement statement = statementIterator.next().getResolvedStatement().get();

    Set<ColumnLineage> lineageEntries = ColumnLineageExtractor.extractColumnLevelLineage(statement);
    System.out.println("Extracted lineage");
    outputLineage(query, lineageEntries);

  }

  public static void main(String[] args) {
    String projectId = "financialreporting-223818";
    BigQueryCatalog catalog = BigQueryCatalog.usingBigQueryAPI(projectId);
    List<String> project_tables = ListAllTables.listAllProjectTables(projectId);
    catalog.addTables(project_tables);

    // catalog.addTables(List.of(
    //     "bigquery-public-data.samples.wikipedia",
    //     "bigquery-public-data.samples.shakespeare"
    // ));

    AnalyzerOptions options = new AnalyzerOptions();
    options.setLanguageOptions(BigQueryLanguageOptions.get());

    ZetaSQLToolkitAnalyzer analyzer = new ZetaSQLToolkitAnalyzer(options);

    Path fp = Path.of("resources/income_expenses_2.txt");
    String query = "";
    try {
      query = Files.readString(fp);
    } catch (IOException e) {
      e.printStackTrace();
    }

    lineageForCustomStatement(catalog, analyzer, query);

    // lineageForCreateTableAsSelectStatement(catalog, analyzer);
    // System.out.println("-----------------------------------");
    // lineageForInsertStatement(catalog, analyzer);
    // System.out.println("-----------------------------------");
    // lineageForUpdateStatement(catalog, analyzer);
    // System.out.println("-----------------------------------");
    // lineageForMergeStatement(catalog, analyzer);
  }

}
