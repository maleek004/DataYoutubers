# DataYoutubers

This project is an end-to-end data solution built entirely in Microsoft Fabric to analyze YouTube channels focused on data-related content. The goal is to help data enthusiasts discover valuable creators by collecting, transforming, and visualizing data on these channels.

## Dashboard

![Dashboard Screenshot](dashboard%20screenshot.jpg)

## Architecture

![Architecture Diagram](architecture%20diagram.png)

## Process Overview

1.  **Data Ingestion:**
    *   A Fabric Warehouse table stores the YouTube handles of the channels to be analyzed.
    *   A Python notebook uses the YouTube API to extract channel statistics and video details, saving them as raw CSV files in a Fabric Lakehouse.

2.  **Data Transformation (Silver Layer):**
    *   Another notebook reads the raw files, performs data transformations, and stores the cleaned data as Delta tables in the same Lakehouse.

3.  **Data Serving (Gold Layer):**
    *   The Lakehouse tables are queried from a Fabric Warehouse to create the gold layer. This approach allows for easy modifications to the data model by editing and re-running the queries.

4.  **Data Visualization & Reporting:**
    *   A custom Power BI semantic model is created with all the required measures.
    *   Power BI Desktop is used to design the final report, which is then published to the web.

5.  **Automation:**
    *   The entire ETL process is automated using a Fabric Pipeline, which includes daily refreshes for the gold layer and the semantic model. This ensures the dashboard is updated daily with the latest channel and video stats.

## Dashboard Features

*   View key metrics for each channel, including channel age, total views, views per video, number of subscribers, and subscriber growth rate.
*   Explore the entire video library of any channel, with sorting options for release date, views, likes, or engagement.
*   Compare multiple channels side by side.
*   See the top-performing channels for each metric.


