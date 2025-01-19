# LeetCode Contest Analytics

## Overview
- A data engineering project to extract, transform, and load LeetCode contest ranking data and contest problems data.
- This project was created to analyze data from past LeetCode contests and top contestants, aiming to set the basis 
for efficient contest practice and further data analytics from each user profile.
- Data were collected from the top **50,000** users of LeetCode global contest ranking and **2334** problems of 
**585** contests.
- Some analytics with visualizations are provided in `analytics/analytics.md`.
- The collected data are stored in the `data/` folder.

## Technology Stack
- Orchestration: **Apache Airflow**
- Storage: **Amazon S3**
- ETL jobs: **Pandas**, **AWS Glue**
- Data warehouse & Analytics: **Amazon Redshift**
- For additional libraries, please consult `requirements.txt`.

## Potential Ideas
- Machine Learning/Deep Learning models.
- User profile analysis and ranking prediction.
- Further analytics and statistics from other LeetCode data.
- Data pipelines and analytics for other platforms.