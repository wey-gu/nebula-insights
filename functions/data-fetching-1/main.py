"""
This is the Cloud Function in below digram.
data_fetch() will be the entrypoint of the trigerring:

┌──────────────────────────┐
│                          │
│  Google Cloud Scheduler  │
│                          │
└────────────┬─────────────┘
             │                             ┌─────────────────────┐
             │                             │                     │
┌────────────▼─────────────┐   ┌───────────►  GitHub API Server  │
│                          │   │           │                     │
│  Google Cloud Functions  ├───┤           └─────────────────────┘
│                          │   │
└────────────┬─────────────┘   │           ┌─────────────────────────┐
             │                 │           │                         │
             │                 ├───────────►  Docker Hub API Server  │
   ┌─────────▼─────────┐       │           │                         │
   │                   │       │           │                         │
   │  Google BigQuery  │       │           └─────────────────────────┘
   │                   │       ├───────────► ...
   └─────────▲─────────┘       │           ┌──────────────────┐
             │                 │           │                  │
             │                 └───────────►  Aliyun OSS API  │
  ┌──────────┴───────────┐                 │                  │
  │                      │                 └──────────────────┘
  │  Google Data Studio  │
  │       ┌──┐           │
  │  ┌──┐ │  │ ┌──┐      │
  │  │  │ │  │ │  │      │
  │  │  │ │  │ │  │      │
  └──┴──┴─┴──┴─┴──┴──────┘

In a closer look, it should be like this, where Google Cloud Storage is involved to:
1. As conf file bucket
2. As Archive Bucket in JSON files
3. As BigQuery Data Loading JSON files Source

                                              ┌─────────────────────┐
                                              │                     │
   ┌──────────────────────────┐   ┌───────────►  GitHub API Server  │
   │                          │   │           │                     │
   │  Google Cloud Functions  ◄───►           └─────────────────────┘
   │                          │   │
   └────────────┬─────────────┘   │           ┌─────────────────────────┐
                │                 │           │                         │
                │                 ├───────────►  Docker Hub API Server  │
   ┌────────────┴────────────┐    │           │                         │
   │                         │    │           │                         │
   │  Google Cloud Storage   │    │           └─────────────────────────┘
   │                         │    ...
   └────────────┬────────────┘    │           ┌──────────────────┐
                │                 │           │                  │
                │                 └───────────►  Aliyun OSS API  │
      ┌─────────▼─────────┐                   │                  │
      │                   │                   └──────────────────┘
      │  Google BigQuery  │
      │                   │
      └───────────────────┘


"""


import base64
import datetime
import json
import pprint
import requests

from google.cloud import storage, bigquery

from github import Github, RateLimitExceededException, GithubException
from urllib3 import Retry

from urllib.parse import urlparse, parse_qs


BUCKET = "nebula-insights"
GCS_RECORD_NAME = {
    "all_external_contributors": "all_external_contributors.json",
    "new_contributors": "new_contributors.json",
    "internal_contributors": "internal_contributors.json"
}

GCP_PROJECT = "nebula-insights"
BQ_DATASET = "nebula_insights"
BQ_TABLE_NAME = {
    "github_contributors": "github_contributor_records"
}
GCP_LOCATION = "asia-east2"

GH_ORG = "vesoft-inc"
DH_USER = "vesoft"
DH_RETRY = 5
DEBUG = False

GH_REPO_EXCLUDE_LIST = [
    "nebula-third-party",
    "nebula-chaos",
    "github-statistics",
    "nebula-community",
    ".github"
]

MERGED_TEMPLATE = "repo:{org}/{repo} is:pr merged:{left}..{right}"

REPORT_REPO = "nebula-community"


class DataFetcher:
    """
    Fetch Data from different sources and sink into datawarehouse.
    """
    def __init__(self):
        self.conf = dict()
        self.github_stats = dict()
        self.dockerhub_stats = dict()
        self.aliyunoss_stats = dict()
        self.maven_stats = dict()
        self.pypi_stats = dict()
        self.go_stats = dict()
        self.all_external_contributors = {}
        self.new_contributors = {}
        self.internal_contributors = {}
        self.external_pull_requests = {}
        self.internal_pull_requests = {}
        self.team_members = set()
        self.s_client = storage.Client()
        self.bucket = self.s_client.get_bucket(BUCKET)
        self.parse_conf()
        self.report_repo = None

    def parse_conf(self):
        """conf for data fetching policies config or fetching API credentials
        ideally it's a file stored in Google Cloud Storage
        """
        blob = self.bucket.blob('conf/config.json')
        data = json.loads(blob.download_as_string(client=None))
        self.conf.update(data)

    def get_sink_credential(self):
        """credential if needed for sinking to big query"""
        pass

    def get_github_sleep_time(self, g):
        core_rate_limit = g.get_rate_limit().core
        reset_timestamp = calendar.timegm(core_rate_limit.reset.timetuple())
        sleep_time = reset_timestamp - calendar.timegm(
            time.gmtime()) + 5  # add 5 sec to ensure the rate limit has been reset
        return sleep_time

    def get_data(self):
        """from github API, dockerhub API, etc."""

        print(f"[INFO] { datetime.datetime.now() } "
              f"Started fetching data from github")
        yesterday = str(self.get_yesterday())
        today = str(datetime.datetime.now().date())
        self.get_data_from_github(yesterday, today)

    def get_data_from_github(self, left=None, right=None):

        token = self.conf.get("github_token")
        g = Github(login_or_token=token, timeout=60, retry=Retry(
                total=10, status_forcelist=(403, 500, 502, 504),
                backoff_factor=0.3))
        org_str = self.conf.get("github_orgnization", GH_ORG)
        org = g.get_organization(org_str)
        repos = org.get_repos()
        team_id = int(self.conf.get("team_id"))
        self.get_team_members(org, team_id)
        for repo in repos:
            repo_key = f"{ org.login }/{ repo.name }"
            if repo.name == REPORT_REPO:
                self.report_repo = repo
            if repo.name not in GH_REPO_EXCLUDE_LIST:
                self.get_github_contributors(g, org_str, repo, left, right,
                    excluded_members=self.team_members)
        if not self.all_external_contributors:
            pass  # need to wire the notification here

    def get_team_members(self, org, team_id):
        team = org.get_team(team_id)
        members = team.get_members()
        for member in members:
            self.team_members.add(member.id)

    def get_contributors(self, gh, orgName, repo, left, right,
            excluded_members=None):
        merged_issue = iter(gh.search_issues(MERGED_TEMPLATE.format(
                org=orgName,
                repo=repo.name,
                left=left,
                right=right),
            "created", "desc"))

        timeIndex = None
        contributors = {}
        contributor_count = {}
        while True:
            try:
                issue = next(merged_issue)
                if DEBUG:
                    print(f"[DEBUG] issue fetched: {issue}")
                if excluded_members is None or issue.user.id not in excluded_members:
                    self.external_pull_requests.setdefault(issue.user.login, set())
                    self.external_pull_requests[issue.user.login].add(issue.html_url)
                    timeIndex = issue.created_at
                    contributor_count[issue.user.login] = contributor_count.get(
                        issue.user.login, 0) + 1
                    if self.is_new_contributor(
                            repo, issue.user.login, contributors,
                            excluded_members, contributor_count):
                        self.new_contributors[repo.name].append(issue.user.login)
                    if DEBUG:
                        print(f"[DEBUG] last issue created at: {timeIndex}")
                else:
                    self.internal_pull_requests.setdefault(issue.user.login, set())
                    self.internal_pull_requests[issue.user.login].add(issue.html_url)
            except StopIteration:
                if DEBUG:
                    print(f"[DEBUG] exception StopIteration hit\n")
                today = datetime.date.today().strftime('%Y-%m-%d')
                if timeIndex and timeIndex >= datetime.datetime.strptime(
                        left, '%Y-%m-%d') \
                        and timeIndex <= datetime.datetime.strptime(
                            today, '%Y-%m-%d'):
                    newStartTime = timeIndex + datetime.timedelta(days=1)
                    if DEBUG:
                        print(f"[DEBUG] newStartTime: {newStartTime}")
                    self.get_contributors(
                        gh, orgName, repo,
                        newStartTime.strftime('%Y-%m-%d'), right,
                        excluded_members=excluded_members)
                    break
                timeIndex = None
                break  # loop end
            except RateLimitExceededException:
                sleep_time = get_sleep_time(gh)
                if DEBUG:
                    print(f"[DEBUG] RateLimitExceeded, sleep {sleep_time} sec")
                time.sleep(sleep_time)
                continue

    def is_new_contributor(self, repo, contributor_login,
            contributors, excluded_members, contributor_count):
        if not contributors:
            for contributor in repo.get_contributors():
                if contributor.id not in excluded_members:
                    contributors[contributor.login] = contributor
            if DEBUG:
                print(f"[DEBUG] fetching {repo.name} contributors: {contributors}")
        if contributor_login not in contributors:
            # The contributors API is not updated, in case it's not included
            # Consider it's a new contributor
            if DEBUG:
                print(f"{contributor_login} not in contributors")
            return True
        if contributors[contributor_login].contributions == 1:
            if DEBUG:
                print(f"{contributor_login} contributions count is 1")
            return True
        else:
            if contributors[contributor_login].contributions <= contributor_count.get(
                    contributor_login):
                if DEBUG:
                    print(f"{contributor_login} contributors[contributor_login].contributions: {contributors[contributor_login].contributions}")
                return True
        return False

    def get_github_contributors(self, gh, orgName, repo, left, right,
            excluded_members=None):

        if DEBUG:
            print(f"[DEBUG] fetching contributors under repo: {repo.name}")
        self.new_contributors[repo.name] = []
        self.get_contributors(gh, orgName, repo, left, right, excluded_members)

        if DEBUG:
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(self.external_pull_requests)

        self.all_external_contributors[repo.name] = dict(
            self.external_pull_requests)
        new_contributors = self.new_contributors[repo.name]
        self.new_contributors[repo.name] = {
            contr: pr for contr, pr in self.external_pull_requests.items()
                if contr in new_contributors
            }
        self.internal_contributors[repo.name] = dict(
            self.internal_pull_requests)
        if DEBUG:
            print(f"----------{repo.name}--------------")
            print("self.external_pull_requests")
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(self.external_pull_requests)

            print("new_contributors")
            pp.pprint(new_contributors)

            print(" self.internal_pull_requests")
            pp.pprint( self.internal_pull_requests)

        self.external_pull_requests = {}
        self.internal_pull_requests = {}

    def save_str_to_gcs_ascii(self, bucket, string_obj, filename):
        """
        Reference:
        https://googleapis.dev/python/storage/latest/blobs.html
            #google.cloud.storage.blob.Blob.upload_from_string
        """
        blob = bucket.blob(filename)
        blob.upload_from_string(data=string_obj, content_type='application/json')

    def get_yesterday(self):
        return (datetime.datetime.now() - datetime.timedelta(1)).date()

    def archive_github_data(self, folder):
        self.save_str_to_gcs_ascii(
            bucket=self.bucket,
            string_obj=str(self.all_external_contributors),
            filename=f"{ folder }/{ GCS_RECORD_NAME['all_external_contributors'] }")

        self.save_str_to_gcs_ascii(
            bucket=self.bucket,
            string_obj=str(self.new_contributors),
            filename=f"{ folder }/{ GCS_RECORD_NAME['new_contributors'] }")

        self.save_str_to_gcs_ascii(
            bucket=self.bucket,
            string_obj=str(self.internal_contributors),
            filename=f"{ folder }/{ GCS_RECORD_NAME['internal_contributors'] }")

    def archive_data(self):
        """
        Archive Data to GCS Bucket
        """
        # records/2021-04-21
        folder = f"records/{ datetime.datetime.now().date() }"
        self.archive_github_data(folder)
        return folder

    def load_bigquery_from_gcs(self, bq_client, gcs_uri, table_id, job_config):
        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_id,
            location=GCP_LOCATION,  # Must match the destination dataset location.
            job_config=job_config,
        )  # Make an API request.
        return load_job

    # def load_data(self, record_folder):
    #     """
    #     load files like:
    #         gs://nebula-insights/records/2021-04-21/github_release_stats.json
    #         gs://nebula-insights/records/2021-04-21/github_clone_stats.json
    #         gs://nebula-insights/records/2021-04-21/dockerhub_image_stats.json
    #     """
    #     self.get_sink_credential()

    #     # Construct a BigQuery client object.
    #     bq_client = bigquery.Client()
    #     TABLE_ID_PREFIX = f"{ GCP_PROJECT }.{ BQ_DATASET }"
    #     URI_PREFIX = f"gs://{ BUCKET }/{ record_folder }"

    #     # table_id
    #     table_id = f"{ TABLE_ID_PREFIX }.{ BQ_TABLE_NAME['github_clone'] }"

    #     # job_config

    #     # nested, referring to
    #     # https://googleapis.dev/python/bigquery/latest/generated/\
    #     #         google.cloud.bigquery.schema.SchemaField.html
    #     job_config = bigquery.LoadJobConfig(
    #         schema=[
    #             bigquery.SchemaField("repo", "STRING", mode="REQUIRED"),
    #             bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    #             bigquery.SchemaField("count", "INTEGER", mode="REQUIRED"),
    #             bigquery.SchemaField("tag", "STRING", mode="REQUIRED"),
    #             bigquery.SchemaField(
    #                 "assets", "RECORD", mode="REPEATED",
    #                 fields=[
    #                     bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    #                     bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
    #                     bigquery.SchemaField("count", "INTEGER", mode="REQUIRED"),
    #                     ])
    #         ],
    #         source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    #     )

    #     # uri
    #     gcs_uri = f"{ URI_PREFIX }/{ GCS_RECORD_NAME['github_clone'] }"

    #     print(f"[INFO] { datetime.datetime.now() } "
    #           f"Started data loading to BigQuery")

    #     job = self.load_bigquery_from_gcs(
    #         bq_client, gcs_uri, table_id, job_config)

    #     try:
    #         job.result()
    #     except:
    #         print(f"[ERROR] { datetime.datetime.now() } "
    #               f"Failed during data loading to BigQuery: { job.errors }")


    def send_issue(self):
        label = self.report_repo.get_label("Weekly Report")
        self.report_repo.create_issue(
            title=f"Weekly Report {datetime.datetime.now().date()}",
            body="",  # To Be added
            labels=[label])

def data_fetch(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    dafa_fetcher = DataFetcher()

    dafa_fetcher.get_data()

    record_folder = dafa_fetcher.archive_data()

    # dafa_fetcher.load_data(record_folder)
