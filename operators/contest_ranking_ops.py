import ast
import pandas as pd
import requests

from utils.queries import contest_ranking_query
from utils.constants import URL, OUTPUT_PATH


def extract_contest_ranking(num_pages):
    """Extracts raw data in all pages"""
    responses = []
    for i in range(num_pages):
        # Get response for each page
        response = requests.post(URL, json=contest_ranking_query(i + 1)).json()["data"]["globalRanking"]["rankingNodes"]
        responses.extend(response)
    output_path = f"{OUTPUT_PATH}/raw/sample_contest_ranking.csv"  # Local file path for sample output data
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
    df["country"] = df["country"].fillna("Unknown")
    df = df.drop(columns=["ranking", "user", "dataRegion"])  # Unnecessary columns
    df = df.rename(columns={  # Rename columns for readability
        "currentRating": "rating",
        "currentGlobalRanking": "global_ranking",
    })

    output_path = f"{OUTPUT_PATH}/processed/sample_contest_ranking.csv"
    df.to_csv(output_path, index=False)
    return output_path
