import json
import requests
import os
import time
import requests
from requests.exceptions import RequestException


def make_request(endpoint, params=None):
    base_url = "https://api.talkwalker.com/api/v1"
    headers = {
        "accept": "application/json",
    }
    try:
        response = requests.get(
            f"{base_url}/{endpoint}",
            params=params,
            headers=headers,
        )

        if response.status_code != 400:
            response.raise_for_status()  # Raise an exception for 4xx or 5xx errors

        return response.json()
    except RequestException as e:
        print(f"Request Exception: {str(e)}")
        return None


def retry_request(endpoint, params=None, max_retries=3):
    retries = 0
    while retries < max_retries:
        response = make_request(endpoint, params)
        if response:
            return response
        retries += 1
        print(f"Retrying... (Attempt {retries}/{max_retries})")
        time.sleep(1)  # Wait for a second before retrying
    return None


def search_value_in_header(header, target_value):
    # Check if "v" is in the header dictionary
    if "v" in header:
        values = header["v"]
        # Check if the target_value is in the list of values
        if target_value in values:
            # Return the index of the target_value in the list
            return values.index(target_value)
    # Return -1 if the target_value was not found
    return -1


def search_account_id(response, target_account_id):
    # Extract the 'result_accinfo' dictionary from the response
    result_accinfo = response.get("result_accinfo", {})

    # Extract the 'projects' list from 'result_accinfo'
    projects = result_accinfo.get("projects", [])

    # Check if 'target_account_id' is present in any project's 'account_id'
    for project in projects:
        if project.get("id") == target_account_id:
            return True

    # If 'target_account_id' is not found in any project
    return False


# def get_credits_estimation(api_token, topic_id, project_id):
#     base_url = "https://api.talkwalker.com/api/v1"

#     def make_request(endpoint, params=None):
#         headers = {
#             "accept": "application/json",
#         }

#         try:
#             response = requests.get(
#                 f"{base_url}/{endpoint}",
#                 params=params,
#                 headers=headers,
#             )

#             response.raise_for_status()  # Raise an exception for 4xx or 5xx errors

#             return response.json()
#         except RequestException as e:
#             print(f"Request Exception: {str(e)}")
#             return None

#     def retry_request(endpoint, params=None, max_retries=3):
#         retries = 0
#         while retries < max_retries:
#             response = make_request(endpoint, params)
#             if response:
#                 return response
#             retries += 1
#             print(f"Retrying... (Attempt {retries}/{max_retries})")
#             time.sleep(1)  # Wait for a second before retrying
#         return None

#     # API 1: Get status credits
#     status_credits_endpoint = "status/credits"
#     status_credits_params = {"access_token": api_token}
#     status_credits_response = retry_request(
#         status_credits_endpoint, status_credits_params
#     )

#     credits_status = {}
#     if status_credits_response:
#         credits_status["available_credits"] = (
#             status_credits_response["result_creditinfo"]["remaining_credits_monthly"]
#             or 0
#         )

#     # API 2: Get search results
#     search_results_endpoint = f"search/p/{project_id}/histogram/published"
#     params = {
#         "interval": "720d",
#         "timezone": "UTC",
#         "breakdown": "topic",
#         "access_token": api_token,
#     }

#     search_results_response = retry_request(search_results_endpoint, params)

#     if search_results_response:
#         index = search_value_in_header(
#             search_results_response["result_histogram"]["header"], topic_id
#         )

#         credits_status["required_credits"] = (
#             search_results_response["result_histogram"]["data"][0]["v"][index]
#             if index > 0
#             else 0
#         )

#     credits_status["enough_credits_available"] = (
#         credits_status["available_credits"] - credits_status["required_credits"]
#     ) > 0
#     return credits_status


def is_valid_project_id(api_token, project_id):
    # API 2: Get search results
    search_results_endpoint = f"search/info"
    search_results_params = {
        "access_token": api_token,
    }
    response = retry_request(search_results_endpoint, search_results_params)

    return search_account_id(response, project_id)


def get_credits_estimation(api_token, topic_id, project_id):
    # API 1: Get status credits
    status_credits_endpoint = "status/credits"
    status_credits_params = {"access_token": api_token}
    status_credits_response = retry_request(
        status_credits_endpoint, status_credits_params
    )

    credits_status = {}
    if status_credits_response:
        credits_status["available_credits"] = (
            status_credits_response["result_creditinfo"]["remaining_credits_monthly"]
            or 0
        )

    # API 2: Get search results
    search_results_endpoint = f"search/p/{project_id}/results"
    search_results_params = {
        "offset": 0,
        "hpp": 0,
        "sort_by": "engagement",
        "sort_order": "desc",
        "hl": True,
        "pretty": False,
        "topic": topic_id,
        "access_token": api_token,
    }
    search_results_response = retry_request(
        search_results_endpoint, search_results_params
    )

    if search_results_response:
        if search_results_response.get("result_error"):
            credits_status["required_credits"] = -1
        else:
            credits_status["required_credits"] = (
                search_results_response["pagination"]["total"] or 0
            )

    credits_status["enough_credits_available"] = (
        credits_status["available_credits"] - credits_status["required_credits"]
    ) > 0

    # special case: if the required_credits is -1, it means the topic_id is invaid

    credits_status["enough_credits_available"] = (
        False if credits_status["required_credits"] <= 0 else True
    )

    return credits_status