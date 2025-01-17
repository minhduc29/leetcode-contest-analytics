import ast
import logging
import pandas as pd
import requests
from airflow.exceptions import AirflowFailException

from utils.queries import contest_ranking_query
from utils.constants import URL, OUTPUT_PATH, BUCKET_NAME, MAX_ATTEMPTS


def extract_contest_ranking(num_pages):
    """Extracts raw data in all pages"""
    responses = []
    for i in range(num_pages):
        attempt = 0
        while attempt < MAX_ATTEMPTS:
            try:
                # Get response for each page
                response = requests.post(URL, json=contest_ranking_query(i + 1)).json()["data"]["globalRanking"]["rankingNodes"]
                responses.extend(response)
                break
            except Exception as e:
                logger = logging.getLogger("airflow.task")
                logger.error(e)
                logger.error(f"Attempt {attempt + 1} failed at page: {i + 1}")
                attempt += 1
        else:
            raise AirflowFailException
    # output_path = f"{OUTPUT_PATH}/raw/sample_contest_ranking.csv"  # Local file path for sample output data
    output_path = f"s3://{BUCKET_NAME}/raw/contest_ranking.csv"  # Amazon S3 storage path
    pd.DataFrame(responses).to_csv(output_path, index=False)
    return output_path


def transform_contest_ranking(task_instance):
    """Processes the raw data and makes necessary changes"""
    input_path = task_instance.xcom_pull(task_ids="extract_contest_ranking")
    df = pd.read_csv(input_path)  # Read the raw data

    # Process the ranking column
    df["ranking"] = df["ranking"].apply(eval)  # Convert from string of list to list
    df["contest_count"] = df["ranking"].apply(len)  # Number of contest attended
    df["avg_ranking"] = df["ranking"].apply(lambda x: round(sum(x) / len(x), 2))  # Avg placement across all contests

    # Process the user column
    df["user"] = df["user"].apply(ast.literal_eval)  # String of dictionary to dictionary
    df["username"] = df["user"].apply(lambda x: x["username"])
    df["country"] = df["user"].apply(lambda x: x["profile"]["countryName"])

    df.loc[df["dataRegion"] == "CN", "country"] = "China"  # Fill country for CN users
    df["country"] = df["country"].replace({None: "Unknown", "": "Unknown"})
    df = df.drop(columns=["ranking", "user", "dataRegion"])  # Unnecessary columns
    df = df.rename(columns={  # Rename columns for readability
        "currentRating": "rating",
        "currentGlobalRanking": "global_ranking",
    })

    # output_path = f"{OUTPUT_PATH}/processed/sample_contest_ranking.csv"
    output_path = f"s3://{BUCKET_NAME}/processed/contest_ranking.csv"  # Amazon S3 storage path
    df.to_csv(output_path, index=False)
    return output_path
