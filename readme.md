# Introduction
This repository contains a workflow of extracting loading and transforming of EU public procurement data to receive actionable insights.
Raw data contains public procurement data from all EU countries from 2009 to 2020. 

## Problem Description
Dataset is located on a public [dataset](https://opentender.eu/all/download) contains zipped raw data (csv unzipped) divided by country abreviation and year of procurement.
All data should folow same schema to allow multisection data analytics across countries:
 - largets sectors, where EU was making purchases over years
 - procedures used
 - amount of funds countries spent by year
 - largest tender ever
 - revenue of largest bidders over years


## Technologies used
Cloud: GCP
Infrastructute as Code: Terraform
Data orchestration: Apache Airflow
Data Warehouse: Big Query
Batch Processing: Spark
Visualisation: Google Studio

## Approach to Problem
 1. [Create infrastructure for GCP via Terraform](initial_setup.md):
   - create GCP project and VM to run Airflow
   - install Anaconda, docker and docker compose
 2. [Install Airflow](airflow_instalation.md)
   - create [docker file](./docker/Dockerfile)
   - create [docker-compose.yaml](./docker/docker-compose.yml)
   - create [.env file](./docker/.env)
   - create [requirements.txt](./docker/requirements.txt)
   - run container on VM
 3. [Design a pipeline](pipeline.md)
   - Chceck if data source is alive
   - Parametrize file names 
   - Upload data go datalake on GCS
   - Create Dataproc cluster
   - Transform data with Spark, load it to Big Query
   - Delete cluster
 4. [Visualise in Google Data Studio](visualisation.md)
   - Create a data conncettion to Big Query
   - Design a dashboard

## To reproduce project:
 1. Create VM on Google Cloud
 2. Follow instructions [here](initial_setup.md)
 3. git clone project
 4. Run docker containers
 5. Trigger Airflow
 





