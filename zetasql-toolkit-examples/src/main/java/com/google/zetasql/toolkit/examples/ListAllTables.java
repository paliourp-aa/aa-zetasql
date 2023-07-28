package com.google.zetasql.toolkit.examples;

import java.util.ArrayList;
import java.util.List;

public class ListAllTables {
    public static void main(String[] args) {
        String projectId = "financialreporting-223818";
        List<String> project_tables = listAllProjectTables(projectId);
        System.out.println(project_tables);

    }
    

    public static List<String> listAllProjectTables(String projectId) {
        // Getting all datasets of specified project
        List<String> all_datasets = ListDatasets.listDatasets(projectId);
        //System.out.println("Datasets: " + all_datasets);
        List<String> all_tables = new ArrayList<>();
        // Getting all tables of each dataset
        for (String d : all_datasets) {
            all_tables.addAll(ListTables.listTables(projectId, d));
        }
        return all_tables;
    }
}
