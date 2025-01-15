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
    file_path = f"{OUTPUT_PATH}/raw/sample_contest_ranking.csv"  # Local file path for sample output data
    pd.DataFrame(responses).to_csv(file_path, index=False)
    return file_path
