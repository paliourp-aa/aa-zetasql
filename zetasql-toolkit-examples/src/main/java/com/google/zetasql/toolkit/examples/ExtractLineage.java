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

import com.google.api.client.util.IOUtils;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import com.google.zetasql.toolkit.AnalyzedStatement;
import com.google.zetasql.toolkit.ZetaSQLToolkitAnalyzer;
import com.google.zetasql.toolkit.catalog.bigquery.BigQueryCatalog;
import com.google.zetasql.toolkit.options.BigQueryLanguageOptions;
import com.google.zetasql.toolkit.tools.lineage.ColumnLineageExtractor;

import com.google.zetasql.toolkit.tools.lineage.ColumnEntity;
import com.google.zetasql.toolkit.tools.lineage.ColumnLineage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ExtractLineage {
    public static void writeToFile(File file, String stringToWrite, boolean append) {
        try {
            FileWriter fw = new FileWriter(file, append);
            fw.write(stringToWrite);
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void outputLineage(String query, Set<ColumnLineage> lineageEntries, Boolean printQuery, BigQueryCatalog catalog, ZetaSQLToolkitAnalyzer analyzer, String path) {

        if (printQuery) {
            System.out.println("\nQuery:");
            System.out.println(query);
        }


        // File to save query's lineage
        File lineage_file = new File(path + "/lineage.txt");

        writeToFile(lineage_file, "", false);

    
        // File to save query's columns
        File columns_file = new File(path + "/columns.txt");

        writeToFile(columns_file, "", false);

        System.out.println("\nLineage:");
    
        lineageEntries.forEach(lineage -> {
            String target_line = lineage.target.table + "." + lineage.target.name + "\n";
            System.out.print(target_line);
            
            // Writing to lineage file
            writeToFile(lineage_file, target_line, true);
         
            // writing column to columns file
            writeToFile(columns_file, lineage.target.name + "\n", true);

         
            for (ColumnEntity parent : lineage.parents) {
                String parent_line = "\t\t<-" + parent.table + "." + parent.name + "\n";
                System.out.print(parent_line);
                
                // Writing to lineage file
                writeToFile(lineage_file, parent_line, true);
            }
        });
        System.out.println();
        System.out.println();
    }


    private static String getType(String projectId, String datasetId, String viewId) {
        BigQuery bq = BigQueryOptions.getDefaultInstance().getService();

        TableId tableId = TableId.of(projectId, datasetId, viewId);

        Table table = bq.getTable(tableId);
    
        if (table.getDefinition().getType().toString().trim().equals("VIEW")) {
            return "[VIEW]";
        } else if (table.getDefinition().getType().toString().trim().equals("TABLE")) {
            return "[TABLE]";
        } else {
            return "[OTHER]";
        }
    }

    private static void getLineage(BigQueryCatalog catalog, ZetaSQLToolkitAnalyzer analyzer, String query, String projectId, String datasetId, String viewId) {
    
        // Make file directory for query
        String path = "analyzed_queries/" + projectId + "." + datasetId + "." + viewId;
        File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    
        // File to save related tables to query
        File tables_file = new File(path + "/tables.txt");

        String type = getType(projectId, datasetId, viewId);    

        // Writing original table/ view name to file
        writeToFile(tables_file, projectId + "." + datasetId + "." + viewId + " " + type + "\n", false);

        System.out.println("Getting tables related to query...");

        Pattern pattern = Pattern.compile("(?i)FROM\\s*`.*?`|(?i)JOIN\\s*`.*?`");

        Matcher matcher = pattern.matcher(query);
        while (matcher.find()) {
            String table_name = matcher.group().trim().replace("FROM ", "").replace("from ", "").replace("JOIN ", "").replace("join ", "").replace("`", "");
            String parts[] = table_name.split("\\.");
            String table_type = getType(parts[0], parts[1], parts[2]);
            writeToFile(tables_file, "\t" + table_name + " " + table_type + "\n", true);
        }

        System.out.println("You can see the tables at: " + path + "/tables.txt");
    
        Iterator<AnalyzedStatement> statementIterator = analyzer.analyzeStatements(query, catalog);

        while (statementIterator.hasNext()) {
            AnalyzedStatement analyzedStatement = statementIterator.next();

            if (analyzedStatement.getResolvedStatement().isPresent()) {
                ResolvedStatement statement = analyzedStatement.getResolvedStatement().get();

                Set<ColumnLineage> lineageEntries = ColumnLineageExtractor.extractColumnLevelLineage(statement);

                System.out.println("Extracting column lineage...");
                outputLineage(query, lineageEntries, false, catalog, analyzer, path);

                // Read line from file
                BufferedReader reader;
                try {
                    reader = new BufferedReader(new FileReader(path + "/tables.txt"));
                    String line = reader.readLine();
                    while (line != null) {
                        System.out.println(line);
                        if (line.contains("[VIEW]") && line.contains("\t")) {
                            line = line.replace("[VIEW]", "");
                            String[] nextQuery_parts = line.trim().split("\\.");
                            // Next query 
                            String nextQuery = GetViewQuery.getCreateTableStatement(nextQuery_parts[0], nextQuery_parts[1], nextQuery_parts[2]);
                            // Calling get lineage for next query 
                            getLineage(catalog, analyzer, nextQuery, nextQuery_parts[0], nextQuery_parts[1], nextQuery_parts[2]);
                            
                        }
                        // Reading next line
                        line = reader.readLine();
                        
                    }
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }

    }

    public static void main(String[] args) {
        String projectId = "financialreporting-223818";
        String datasetId = "reporting_model";
        String viewId = "aa_all_payroll_mapped_3";

        BigQueryCatalog catalog = BigQueryCatalog.usingBigQueryAPI(projectId);
        List<String> project_tables = ListAllTables.listAllProjectTables(projectId);
        catalog.addTables(project_tables);
    

        AnalyzerOptions options = new AnalyzerOptions();
        options.setLanguageOptions(BigQueryLanguageOptions.get());

        ZetaSQLToolkitAnalyzer analyzer = new ZetaSQLToolkitAnalyzer(options);

    
        String query = "";
        // Query from text file
        // Path fp = Path.of("resources/income_expenses_2.txt");
        // try {
        //   query = Files.readString(fp);
        // } catch (IOException e) {
        //   e.printStackTrace();
        // }

        // Query from BQ view
        query = GetViewQuery.getCreateTableStatement(projectId, datasetId, viewId);

        getLineage(catalog, analyzer, query, projectId, datasetId, viewId);

    }

}
