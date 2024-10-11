import os
import sys
import logging
from datetime import datetime
from .driver import Driver
from driver_library_{{ org_name }}_{{ venture_name }}.driver_library.utils.core.environment import CheckEnvironment

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Constants:
    # per job parameters
    docker_variables = ["from_date", "to_date", "task_id", "project_id", "topic_id", "get_news_links"]

    # cloud parameters (from kubernetes secrets)
    secret_variables = ["API_KEY", "TWITTER_TOKEN"]

    # configuration parameters
    configuration_variables = ["max_retries", "page_size", "bucket_location", "tw_backfill_start_date",
                                "tw_project_id", "tw_topic_id", "tw_download_news"]


def parse_date(date_time_value: str) -> str:

    if not date_time_value:
        raise ValueError("Date was not supplied.")

    try:

        logger.info(f'original date = {date_time_value}')
        dt = date_time_value.split()[0]
        logger.info(f'date portion = {dt}')

        obj = datetime.strptime(dt, "%Y-%m-%d")
        # Return the date portion as a string
        rc = obj.strftime("%Y-%m-%d")

        logger.info(f'validated date = {rc}')

        return rc

    except ValueError:
        raise ValueError(f"Supplied date [{date_time_value}] has incorrect format.")


def fix_scheduled_inputs(args: dict, env_vars: dict) -> dict:

    if args["scheduled"]:

        logger.info("scheduled run - will take certain parameters from config map")

        # fix up certain parameters

        if args["from_date"].casefold() == "None".casefold():
            logger.info(f'From date is None. Changed to tw_backfill_start_date = {env_vars["tw_backfill_start_date"]}')
            args["from_date"] = env_vars["tw_backfill_start_date"]

        args["project_id"] = env_vars["tw_project_id"]
        args["topic_id"] = env_vars["tw_topic_id"]
        args["get_news_links"] = env_vars["tw_download_news"]

        logger.info(f"scheduled run - project_id = {args['project_id']} from config map")
        logger.info(f"scheduled run - topic_id = {args['topic_id']} from config map")
        logger.info(f"scheduled run - get_news_links = {args['get_news_links']} from config map")

    else:
        logger.info("manual run - scheduled parameters are ignored")

    logger.info(f'fix_scheduled_inputs return {args}')
    return args


def get_talkwalker_inputs(args: dict, env_vars: dict) -> dict:
    args_dict = {}

    for key in Constants.docker_variables:
        args_dict[key] = args[key]
        logger.info(f"Found variable {key} = [{args_dict[key]}] .")

    args_dict = fix_scheduled_inputs(args, env_vars)

    # strip time off from date values

    args_dict["from_date"] = parse_date(args_dict["from_date"])
    args_dict["to_date"] = parse_date(args_dict["to_date"])

    return args_dict


def run(args: dict) -> dict:
    """
    Main entry point into talkwalker driver called from Arflow DAG.
    :param args: args is a dictionary with all inputs needed to run the program
    :return: return value is a dictionary with oll outputs (s3 location in this case)
    """

    logger.info(f"Talkwalker - started.")

    logger.info(f'input args = {args}')

    env_vars = CheckEnvironment.get_env(Constants.secret_variables + Constants.configuration_variables)

    # logger.info(f'env vars = {env_vars}') # TODO: Display variables without sensitive information only

    if not CheckEnvironment.check_keys(Constants.secret_variables + Constants.configuration_variables, env_vars):
        logger.error(f"Required environment variables for talkwalker are not present.")
        sys.exit(1)

    if not CheckEnvironment.check_keys(Constants.docker_variables, args):
        logger.error(f"Required variables for talkwalker are not present.")
        sys.exit(1)

    args_dict = get_talkwalker_inputs(args, env_vars)

    all_vars = {**args_dict, **env_vars}

    driver = Driver()

    rc = driver.run(all_vars)

    logger.info(f"Talkwalker - completed.")

    return rc
