import logging
import pandas as pd
import requests
from airflow.exceptions import AirflowFailException
from collections import defaultdict

from utils.queries import contest_problems_query, problem_tags_query
from utils.constants import URL, OUTPUT_PATH, BUCKET_NAME, MAX_ATTEMPTS


def etl_contest_problems(num_pages):
    """Extracts, transforms, and loads contests' problems in all pages"""
    responses = []
    for i in range(num_pages):
        attempt = 0
        while attempt < MAX_ATTEMPTS:
            try:
                # Get response for each page
                response = requests.post(URL, json=contest_problems_query(i + 1)).json()["data"]["pastContests"]["data"]
                for contest in response:
                    responses.extend(parse_contest_problems(contest))  # Transform response data to optimized format
                break  # Successful operation
            except Exception as e:
                logger = logging.getLogger("airflow.task")
                logger.error(e)
                logger.error(f"Attempt {attempt + 1} failed at page: {i + 1}")
                attempt += 1
        else:
            raise AirflowFailException
    # output_path = f"{OUTPUT_PATH}/raw/contest_problems.csv"
    output_path = f"s3://{BUCKET_NAME}/raw/contest_problems.csv"
    pd.DataFrame(responses).to_csv(output_path, index=False)  # Load the data to the destination storage
    return output_path


def parse_contest_problems(contest_data):
    """Parses the contest data in custom format"""
    return [
        {
            "contest": contest_data["titleSlug"],
            "problem": problem["titleSlug"]
        } for problem in contest_data["questions"]
    ]


def etl_problem_tags(task_instance):
    """Extracts, transforms, and loads each problem's tags"""
    input_path = task_instance.xcom_pull(task_ids="etl_contest_problems")
    df = pd.read_csv(input_path)

    counter = defaultdict(int)  # Count the number of each topic tag showing up
    responses = []
    for problem in df["problem"]:
        attempt = 0
        while attempt < MAX_ATTEMPTS:
            try:
                # Get data for each problem
                response = requests.post(URL, json=problem_tags_query(problem)).json()["data"]["question"]
                tags = parse_problem_tags(response)  # Transform data to get the list of tags
                responses.append({
                    "problem": problem,
                    "tags": tags
                })
                for tag in tags:
                    counter[tag] += 1
                break  # Successful operation
            except Exception as e:
                logger = logging.getLogger("airflow.task")
                logger.error(e)
                logger.error(f"Attempt {attempt + 1} failed at problem: {problem}")
                attempt += 1
        else:
            raise AirflowFailException

    # Load tags data to the destination storage
    # output_path = f"{OUTPUT_PATH}/raw/problem_tags.csv"
    output_path = f"s3://{BUCKET_NAME}/raw/problem_tags.csv"
    pd.DataFrame(responses).to_csv(output_path, index=False)

    # Load tags counter data to the destination storage
    # counter_path = f"{OUTPUT_PATH}/processed/tags_counter.csv"
    counter_path = f"s3://{BUCKET_NAME}/processed/tags_counter.csv"
    pd.Series(counter).to_csv(counter_path, header=False)

    return output_path


def parse_problem_tags(problem_data):
    """Parses the problem data to get list of topic tags"""
    return [tag["name"] for tag in problem_data["topicTags"]]
