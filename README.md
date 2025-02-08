## Overview

Developed an Airflow DAG to automate daily e-commerce sales data processing using Spark. Leveraged SubmitSparkOperator to submit jobs for data transformation. Generated reports with key metrics (sales, customers, products) and stored results in S3 for centralized logging and accessibility, optimizing reporting workflows.

### Data that needed to be processed (0.5 Million Rows)

![Dataset Overview](images/1.png)

### Created DAG that runs daily

![Airflow DAG Execution](images/2.png)

### Spark running locally using Docker

![Spark Running in Docker](images/3.png)

### Graph view of the DAG

![DAG Graph View](images/4.png)

### Using SparkSubmitOperator to submit Spark job

![SparkSubmitOperator Execution](images/5.png)

### Logs of the first task that generated the report using PySpark

![PySpark Task Logs](images/6.png)

### Report saved in S3 Bucket

![Report in S3 Bucket](images/7.png)

### A copy of the report is saved locally too

![Local Report Storage](images/8.png)

### View of the generated report

![Generated Report Preview](images/9.png)

### All configs in one place for easy modifications

![Configuration Settings](images/10.png)

### AWS credentials stored securely as variables in Airflow

![Airflow AWS Credential Storage](images/11.png)

### Connection to Spark saved in Airflow for easy access

![Airflow Spark Connection](images/12.png)

## How to run?

- Clone this repo

```
git clone
```

- Add your AWS creds in Airflow, S3 Bucket path in config.
- Install [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli/)
- Start Airflow and Spark with a single command

```
astro dev start
```
