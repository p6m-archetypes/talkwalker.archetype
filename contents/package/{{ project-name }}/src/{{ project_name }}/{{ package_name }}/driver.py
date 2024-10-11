import os
import logging
import json
import time
import traceback
import requests
from datetime import datetime
from .credits import get_credits_estimation
from .record import TalkwalkerRecord
from .source import TalkwalkerSource
from twitter_{{ org_name }}_{{ venture_name }}.twitter.source import TwitterSource
from driver_library_{{ org_name }}_{{ venture_name }}.driver_library.utils.s3.s3_object_store import S3
from driver_library_{{ org_name }}_{{ venture_name }}.driver_library.utils.md5.MD5Generator import MD5Source


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Constants:
    TWITTER_IDS_COUNT = 100
    DRIVER_NAME = "talkwalker"  # must match postgres database lookup table
    APPLICATION_NAME = DRIVER_NAME
    VERSION = 24
    PARTITION_NUM = 1
    # s3 object key  = raw/{application}/{hash_id}/{from_date}_{to_date}/file_{int}jsonl
    S3_KEY_TEMPLATE_PREFIX = "raw/{}"  # raw/{application} : for downstream drivers with their own names
    S3_KEY_TEMPLATE_POSTFIX = "/{}/{}_{}/file_{}.jsonl"  # /{hash_id}/{from_date}_{to_date}/file_{int}.jsonl
    XCOM_KEY_TEMPLATE_POSTFIX = "/{}/{}_{}/xcom_{}.json"  # /{hash_id}/{from_date}_{to_date}/xcom_{hash_id}.json


class Driver:
    """
    Driver class implements the talkwalker pull logic
    """

    def __init__(self):

        self.logger = logger
        self.object_storage = None
        self.talk_walker = None
        self.output_bucket = None
        self.application_name = f'{Constants.APPLICATION_NAME} v.{Constants.VERSION} '
        self.params: dict = {}
        print(f'{self.application_name} initialized.')

    def initialize_buckets(self) -> None:
        """Validate all three S3 bucket parameters variables"""

        self.output_bucket = self.params["bucket_location"]

        if not self.output_bucket:
            self.logger.error("bucket_location is missing.")
            exit(1)

    def authenticate_s3(self):
        """Authenticate S3 credentials"""

        obj_storage = S3()
        if not obj_storage.authenticate():
            self.logger.error(
                "AWS Credentials are missing."
            )
            exit(1)

        self.object_storage = obj_storage

    def upload_file(self, file_path, bucket_name, key_name: str) -> bool:
        """This method copies file from a local directory to the text bucket"""

        self.logger.info(f"file path = {file_path}")

        self.logger.info(f"{self.application_name} - Uploading file {file_path} to bucket {bucket_name}")
        if not self.object_storage.upload_file(file_path, bucket_name, key_name):
            self.logger.error(f"File {file_path} copy to bucket {bucket_name} failed.")
            return False
        else:
            self.logger.info(f"File {file_path} was copied to bucket {bucket_name}.")
            return True

    def transform_tweet_data(self, tweet_data, item):
        """Method to transform the talkwalker item, tweet data and return it as dict"""
        created_at = tweet_data["created_at"].replace(" ", "").replace("\n", "")
        dt_obj = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        created_epoch_unix = int(dt_obj.timestamp())

        data = {**item, **tweet_data}
        # NOTE: only replace the published date with twitter date if the talkwalker date
        # is not available or is invalid
        if data["published"] == -1 or data["published"] == 0:
            data["published"] = created_epoch_unix
            data["x-p6m-publish-source"] = "twitter"
        text = data.pop("text")
        data["content"] = text
        data["word_count"] = len(text.split())
        data["external_provider_attributes"] = tweet_data
        data["url"] = tweet_data["id"]

        if not tweet_data.get("author_id"):
            self.logger.warning(f'Warning - author id is null for tweet id {tweet_data["id"]}')

        # print(f'tweet text inside merge =  {data["body"]}')
        return data

    def save_data_to_file(self, data, jsonl_filename):
        with open(jsonl_filename, "a") as f:
            for item in data:
                # print(item)
                f.write(TalkwalkerRecord.parse_obj(item).model_dump_json() + "\n")

    @staticmethod
    def get_item_by_id(items, external_id):
        for item in items:
            if item["external_id"] == external_id:
                return item
        return ""

    def merge_tweet_data(self, items, error_file_path):

        page_size = self.params["page_size"]
        max_retries = int(self.params["max_retries"])
        twitter_token = self.params["TWITTER_TOKEN"]

        twitter = TwitterSource(page_size, max_retries, twitter_token)

        tweets_data = twitter.get_tweets_by_ids(
            [item["external_id"] for item in items], error_file_path
        )

        data = []
        if len(tweets_data["errors"]):
            self.logger.info(
                f'NOT FOUND BEFORE - second try: {[error["value"] for error in tweets_data["errors"]]}'
            )
            time.sleep(15)
            tweets_second = twitter.get_tweets_by_ids(
                [error["value"] for error in tweets_data["errors"]], error_file_path
            )

            tweets_data["data"] = tweets_data["data"] + tweets_second["data"]
            tweets_data["errors"] = tweets_second["errors"]

            self.logger.info(
                f"NOT FOUND AFTER - second try: {[error['value'] for error in tweets_data['errors']]}"
            )
            # self.talk_walker.log_error(f"Twitter errors {tweets_data['errors']}")

            if len(tweets_data["errors"]):
                self.logger.info(
                    f"NOT FOUND BEFORE - third try: {[error['value'] for error in tweets_data['errors']]}"
                )
                time.sleep(15)
                tweets_third = twitter.get_tweets_by_ids(
                    [error["value"] for error in tweets_data["errors"]], error_file_path
                )

                tweets_data["data"] = tweets_data["data"] + tweets_third["data"]
                tweets_data["errors"] = tweets_third["errors"]

                self.talk_walker.twitter_errors = self.talk_walker.twitter_errors + len(
                    tweets_data["errors"]
                )
                self.logger.info(
                    f"NOT FOUND AFTER - third try: {[error['value'] for error in tweets_data['errors']]}"
                )
                # self.talk_walker.log_error(f"Twitter errors {tweets_data['errors']}")
            self.talk_walker.twitter_errors = self.talk_walker.twitter_errors + len(
                tweets_data["errors"]
            )

        # TODO - the loop below should iterate on TW items
        # instead of tweet items as not all twitter hydration will succeed.
        # this way all un-hydrated twitter items from TW will be present in the output,
        # at lease (even if un-hydrated).

        for tweet in tweets_data["data"]:
            try:
                original_tw_item = self.get_item_by_id(items, tweet["id"])
                merged_items = self.transform_tweet_data(tweet, original_tw_item)
                data.append(merged_items)

            except Exception as e:
                self.logger.error(f"Exception in twitter TW merge!")
                self.logger.exception(e)

        for tweet in tweets_data["errors"]:
            try:
                original_tw_item = self.get_item_by_id(items, tweet["value"])
                original_tw_item["twitter_error"] = tweet
                original_tw_item.pop("x-p6m-publish-source", None)
                data.append(original_tw_item)

            except Exception as e:
                self.logger.error(f"Exception in twitter Errors and TW merge!")
                self.logger.exception(e)

        self.logger.info(
            f"Tweets merged. TW = {len(items)}. valid = {len(tweets_data['data'])}.  invalid = {len(tweets_data['errors'])} Merged = {len(data)}"
        )
        return data

    def run(self, params: dict) -> dict:
        """
        Main method in Driver class that invokes the entire logic of talkwalker
        :param params: args is a dictionary with all inputs needed to run the program
        :return: return value is a dictionary with oll outputs (s3 location in this case)
        """

        self.params = params

        # logger.info(f'combined args = {params}') TODO: Display variables without sensitive information only

        agent_name = f"{self.application_name}"

        self.logger.info(f'Running application = {agent_name}')

        self.logger.info(f"{self.application_name} Validating access to s3 buckets.")
        self.initialize_buckets()
        self.logger.info(f"{self.application_name} Access to s3 buckets complete.")

        self.logger.info(f"Output Bucket = {self.output_bucket}")

        # NOTE  sample payload
        """

         {
            "project_id": "ad6bc12c-bb4e-4cbd-9d27-3250d40d6305",
            "topic_id": "lp1tech7_gq0y2dnq4fgv",
            "get_news_links": false,
            "from_date": "2023-11-16",
            "to_date": "2023-11-15"
         }

        project_id: if the project_id is not provided, the PROJECT_ID form env is used.
        from_date: if from_date is not specified, TODAY's date will be used by default.
        to_date: if to_date is not specified, it defaults to 30 days back from from_date.
        """
        # query = json.loads(task["query"].replace("'", '"'))
        # task_id = task["id"]
        # topic_id = (query.get("topic_id", "")).strip()
        # project_id = (query.get("project_id", "")).strip()

        project_id = params['project_id']
        topic_id = params['topic_id']
        task_id = params['task_id']
        from_date = params['from_date']
        to_date = params['to_date']
        get_news_links = params['get_news_links']

        if topic_id == "":
            self.logger.info(f"missing topic_id - {topic_id}")
            exit(1)

        # Note: If the project_id is not specified, the PROJECT_ID from env will be used.
        if project_id == "":
            project_id = self.params["PROJECT_ID"]

        # we have a topic id to run now, so initialize job specific logger

        timestamp = int(time.time())  # Generate a unique timestamp

        # self.logger is available now as a per job logger
        self.logger.info(
            f"{self.application_name} New task id = {task_id} running at timestamp = {timestamp} topic id = {topic_id}")

        self.authenticate_s3()
        try:

            max_retries = int(self.params["max_retries"])
            access_token = self.params["API_KEY"]
            page_size = self.params["page_size"]

            self.talk_walker = TalkwalkerSource(params, max_retries, page_size, access_token)

            # validate project id and topic id

            project_name, (topic_name, venture_name) = self.talk_walker.get_project_topic_names(project_id, topic_id)

            """
            check if the topic is valid and that we have enough credits
            """
            can_exec_topic = get_credits_estimation(
                self.talk_walker.access_token, topic_id, self.talk_walker.project_id
            )
            print(can_exec_topic)
            available_credits = can_exec_topic["available_credits"]
            self.talk_walker.required_credits = can_exec_topic["required_credits"]

            if self.talk_walker.required_credits == -1:
                logging.error(f"invalid topic id: {topic_id}")
                exit(1)

            if not can_exec_topic["enough_credits_available"]:
                logging.error(f"Not enough credits available for: {topic_id}")
                logging.error(
                    f"Available credits: {available_credits}, required credits: {self.talk_walker.required_credits}"
                )
                exit(1)

            self.logger.info(
                f"{self.application_name} Topic: {topic_id},  total items to be retrieved: {self.talk_walker.required_credits}"
            )

            jsonl_filename = f"{Constants.APPLICATION_NAME}_{topic_id}_{timestamp}.jsonl"  # Include timestamp in the filename
            error_filename = f"{Constants.APPLICATION_NAME}_{topic_id}_{timestamp}.errors.txt"  # Include timestamp in the filename

            path = './data'

            # check whether directory already exists
            if not os.path.exists(path):
                os.mkdir(path)
                self.logger.info(f"{self.application_name} Folder {path} created!")
            else:
                self.logger.info(f"{self.application_name} Folder {path} already exists")

            jsonl_file_path = os.path.join(path, jsonl_filename)
            error_file_path = os.path.join(path, error_filename)

            self.logger.info(f'local json file path = {jsonl_file_path}')
            self.logger.info(f'local error file path = {error_file_path}')

            tweet_items = []  # list to hold tweet items for batching

            job_status_update = {
                "total_retrieved": 0,
                "total_twitter": 0,
                "twitter_errors": 0,
                "self.talk_walker.total_saved": 0,
                "latest_errors": [],
            }

            loop_count = 0

            for data in self.talk_walker.retrieve_data():
                for item in data:

                    loop_count += 1

                    if item.get("external_provider", "") == "twitter":
                        tweet_items.append(item)  # add the item to the batch list

                        # If we've reached 100 items, get the tweets and write to the file
                        if len(tweet_items) == Constants.TWITTER_IDS_COUNT:
                            self.logger.info(
                                f"Batched tweeter items = {len(tweet_items)} "
                            )
                            merged_items = self.merge_tweet_data(
                                tweet_items, error_file_path
                            )
                            self.logger.info(
                                f"Merged tweeter items = {len(merged_items)} from original TW items  = {len(tweet_items)}"
                            )
                            self.talk_walker.total_saved += len(merged_items)
                            self.save_data_to_file(merged_items, jsonl_file_path)
                            tweet_items = []
                    else:
                        self.talk_walker.total_saved += 1
                        self.save_data_to_file([item], jsonl_file_path)

                    job_status_update = {
                        "total_retrieved": self.talk_walker.total_item_count,
                        "total_twitter": self.talk_walker.total_twitter_count,
                        "twitter_errors": self.talk_walker.twitter_errors,
                        "total_saved": self.talk_walker.total_saved,
                        "latest_errors": self.talk_walker.get_latest_errors(),
                    }

                    self.logger.info(f"### {self.application_name} status : {job_status_update}")

                    if len(self.talk_walker.get_latest_errors()) != 0:
                        self.logger.info(
                            f'{self.application_name} latest errors : {self.talk_walker.get_latest_errors()}')

            if tweet_items:
                merged_items = self.merge_tweet_data(tweet_items, error_file_path)
                self.talk_walker.total_saved += len(merged_items)
                self.save_data_to_file(merged_items, jsonl_file_path)

            self.logger.info(
                f"### {self.application_name} ### Final Total items retrieved: {self.talk_walker.total_item_count}"
            )
            self.logger.info(
                f"### {self.application_name} Final Total twitter items: {self.talk_walker.total_twitter_count}"
            )
            self.logger.info(
                f"### {self.application_name} ### Final Total items saved: {self.talk_walker.total_saved}"
            )
            self.logger.info(
                f"### {self.application_name} ### Total TalkWalker Items: {self.talk_walker.required_credits}"
            )
            self.logger.info(f'{self.application_name} latest errors : {self.talk_walker.get_latest_errors()}')
            self.logger.info(
                f'{self.application_name} Status : talkwalker job is complete. Next step is to save results to S3 now.')

            # input json for generating MD5 hash
            hash_input = {
                'project_id': project_id,
                'topic_id': topic_id,
                "get_news_links": get_news_links
            }

            self.logger.info(f'hash input = {hash_input}')

            md5 = MD5Source(input_json=hash_input, keys_to_exclude=['from_date', 'to_date'], delimiter='|')
            hash_id = md5.generate_md5_hash()

            self.logger.info(f'generated hash = {hash_id}')

            # s3 object key  = raw/{application}/{hash_id}/{from_date}_{to_date}/file_{int}jsonl

            s3_filled_postfix = Constants.S3_KEY_TEMPLATE_POSTFIX.format(
                hash_id, from_date, to_date, Constants.PARTITION_NUM)

            s3_template = Constants.S3_KEY_TEMPLATE_PREFIX + s3_filled_postfix

            xcom_filled_postfix = Constants.XCOM_KEY_TEMPLATE_POSTFIX.format(
                hash_id, from_date, to_date, hash_id)

            xcom_template = Constants.S3_KEY_TEMPLATE_PREFIX + xcom_filled_postfix

            # object_storage_key_for_results

            s3_jsonl_key_name = s3_template.format(Constants.APPLICATION_NAME)
            xcom_json_key_name = xcom_template.format(Constants.APPLICATION_NAME)

            self.upload_file(jsonl_file_path, self.output_bucket, s3_jsonl_key_name)

            self.logger.info(f'{self.application_name} Status : output has been written to {s3_jsonl_key_name}.')

            data = {
                "output_template": s3_template,
                "xcom_template": xcom_template,
                "talkwalker_output": f"s3://{self.output_bucket}/{s3_jsonl_key_name}",
                "query_hash": hash_id,
                "project_id": params['project_id'],
                "topic_id": params['topic_id'],
                "from_date": params['from_date'],
                "to_date": params['to_date'],
                "project_name": project_name,
                "topic_name": topic_name,
                "vendor_name": "talkwalker",
                "source_format": "json",
                "venture_name": venture_name,
            }

            self.logger.info(f'talkwalker output = {data}')
            # upload xcom as a file to s3
            xcom_file_name = f'xcom_{hash_id}.json'
            with open(xcom_file_name, 'w') as f:
                json.dump(data, f)
            self.upload_file(xcom_file_name, self.output_bucket, xcom_json_key_name)

            self.logger.info(f"{self.application_name} Job Id id = {task_id} completed.")
            self.logger.info(f"\n=========================================\n")
            self.logger.info(
                f"\n{self.application_name} == Results for Job id {task_id} is available at s3://{self.output_bucket}/{s3_jsonl_key_name}  ==\n")
            self.logger.info(f"\n===============Completed=================\n")

            return data

        except (KeyboardInterrupt, TypeError, Exception) as e:

            print(traceback.format_exc())
            self.logger.error(traceback.format_exc())
            self.logger.info(f"Task failed - exception caught : {e}")
            exit(1)
