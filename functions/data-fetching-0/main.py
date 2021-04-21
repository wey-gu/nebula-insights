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
import requests

from google.cloud import storage, bigquery

from github import Github, RateLimitExceededException, GithubException
from urllib3 import Retry

from urllib.parse import urlparse, parse_qs

# docker_hub_client
DOCKER_HUB_API_ENDPOINT = "https://hub.docker.com/v2/"
PER_PAGE = 50
# docker_hub_client

BUCKET = "nebula-insights"
GCS_RECORD_NAME = {
    "github_clone": "github_clone_stats.json",
    "github_release": "github_release_stats.json",
    "dockerhub_image": "dockerhub_image_stats.json"
}

GCP_PROJECT = "nebula-insights"
BQ_DATASET = "nebula_insights"
BQ_TABLE_NAME = {
    "github_clone": "github_clone_records",
    "github_release": "github_release_records",
    "dockerhub_image": "dockerhub_image_records"
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
        self.s_client = storage.Client()
        self.bucket = self.s_client.get_bucket(BUCKET)
        self.parse_conf()

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

    def get_github_clone_stats(self, g, org, repo):
        repo_key = f"{ org.login }/{ repo.name }"
        type_key = "clones"
        while True:
            try:
                if DEBUG:
                    print(f"[DEBUG] { datetime.datetime.now() } "
                          f"get_clones_traffic { repo_key }")
                clones_traffic = repo.get_clones_traffic().get("clones")[:-1]
                clones_stats = { str(item.timestamp.date()):item.count
                    for item in clones_traffic }
                self.github_stats[repo_key][type_key].update(clones_stats)
                break
            except RateLimitExceededException:
                sleep_time = self.get_github_sleep_time(g)
                print(f"[ERROR] { datetime.datetime.now() } "
                      f"RateLimitExceeded, sleep { sleep_time }")
                time.sleep(sleep_time)
                continue
            except GithubException as e:
                if e.status == 403 \
                    and e.data["message"].startswith(
                        "Must have push access"):
                    print(f"[WARN] { datetime.datetime.now() } "
                          f"No Push Access skipping { repo_key }")
                    return
                else:
                    print(f"[ERROR] { datetime.datetime.now() } "
                          f"GithubException on { repo_key }: { e }")
                continue

    def get_github_release_stats(self, g, org, repo):
        repo_key = f"{ org.login }/{ repo.name }"
        type_key = "releases"
        if DEBUG:
            print(f"[DEBUG] { datetime.datetime.now() } "
                  f"get_releases { repo_key }")
        for release in repo.get_releases():
            tag_name = release.tag_name
            while True:
                try:
                    assets_stats = {tag_name: {}}
                    for asset in release.get_assets():
                        if asset.name.endswith(".txt"):
                            continue  # skip checksum files
                        if DEBUG:
                            print(f"[DEBUG] { datetime.datetime.now() } "
                                  f"get_assets { asset.name }")
                        assets_stats[tag_name][asset.name] = asset.download_count
                        self.github_stats[repo_key][type_key].update(assets_stats)
                    break
                except RateLimitExceededException:
                    sleep_time = self.get_github_sleep_time(g)
                    print(f"[ERROR] { datetime.datetime.now() } "
                          f"RateLimitExceeded, sleep { sleep_time }")
                    time.sleep(sleep_time)
                    continue

    def get_data_from_github(self):
        token = self.conf.get("github_token")
        g = Github(login_or_token=token, timeout=60, retry=Retry(
                total=10, status_forcelist=(500, 502, 504),
                backoff_factor=0.3))
        org_str = self.conf.get("github_orgnization", GH_ORG)
        org = g.get_organization(org_str)
        repos = org.get_repos()
        for repo in repos:
            repo_key = f"{ org.login }/{ repo.name }"
            if repo.name not in GH_REPO_EXCLUDE_LIST:
                self.github_stats.setdefault(
                    repo_key, {"clones": {}, "releases": {}})
                self.get_github_clone_stats(g, org, repo)
                self.get_github_release_stats(g, org, repo)
        if not self.github_stats:
            pass  # need to wire the notification here

    def get_data_from_dockerhub(self):
        dh_client = DockerHubClient()
        left_attempts = DH_RETRY
        dockerhub_stats = dict()
        while left_attempts > 0:
            repos = dh_client.get_repos(DH_USER)
            if DEBUG:
                print(f"[DEBUG] { datetime.datetime.now() } "
                      f"dockerhub count: { repos['content']['count'] }")
            if repos.get('code', None) != 200:
                left_attempts -= 1
                if DEBUG:
                    print(f"[DEBUG] { datetime.datetime.now() } "
                          f"left attempt: { left_attempts }")
                continue
            if repos.get('content', {}).get('count', 0) > 0 \
                    and repos.get('content', {}).get('results', None):
                while left_attempts > 0:  # handling all pages of images
                    images = repos['content']['results']

                    def image_key(image):
                        namespace = image.get('namespace', '')
                        name = image['name']
                        return f"{ namespace }/{ name }"

                    new_stats = {
                        image_key(image): image.get('pull_count', 0) \
                            for image in images \
                            if image.get('repository_type', '') == 'image'
                        }
                    dockerhub_stats.update(new_stats)
                    if repos['content']['next'] is None:
                        break
                    else:
                        qs = parse_qs(urlparse(repos['content']['next']).query)
                        repos_new = dh_client.get_repos(
                            DH_USER, page=int(qs['page'][0]))
                        if repos_new.get('code', None) != 200:
                            left_attempts -= 1
                            if DEBUG:
                                print(f"[DEBUG] { datetime.datetime.now() } "
                                      f"left attempt: { left_attempts }")
                            continue
                        else:
                            repos = repos_new
                break
            else:
                # there is no content, end loop now
                break

        if dockerhub_stats:
            self.dockerhub_stats.update(dockerhub_stats)
        else:
            if DEBUG:
                print(f"[DEBUG] { datetime.datetime.now() } "
                      f"dockerhub_stats is empty")
            pass  # need to wire the notification here

    def get_data_from_aliyunoss(self):
        pass

    def get_data(self):
        """from github API, dockerhub API, etc."""

        print(f"[INFO] { datetime.datetime.now() } "
              f"Started fetching data from github")
        self.get_data_from_github()

        print(f"[INFO] { datetime.datetime.now() } "
              f"Started fetching data from dockerhub")
        self.get_data_from_dockerhub()

        print(f"[INFO] { datetime.datetime.now() } "
              f"Started fetching data from aliyunoss")
        self.get_data_from_aliyunoss()

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
        github_clone_list = list()
        github_release_list = list()
        for repo, repo_dict in self.github_stats.items():
            # convert github clone stats
            for date, count in repo_dict.get('clones', {}).items():
                github_clone_record = dict(repo=repo, date=date, count=count)
                github_clone_list.append(f"{ json.dumps(github_clone_record) }")
            for tag, tag_dict in repo_dict.get('releases', {}).items():
                if not tag_dict:
                    continue
                count = 0
                assets = []
                for asset, asset_count in tag_dict.items():
                    count += asset_count
                    assets.append(dict(
                        name=asset,
                        url=f"https://github.com/{ repo }/"
                            f"releases/download/{ tag }/{ asset }",
                        count=asset_count))
                github_release_record = dict(
                    repo=repo, date=str(self.get_yesterday()), tag=tag,
                    count=count, assets=assets)
                github_release_list.append(f"{ json.dumps(github_release_record) }")
        if github_clone_list:
            self.save_str_to_gcs_ascii(
                bucket=self.bucket,
                string_obj="\n".join(github_clone_list),
                filename=f"{ folder }/{ GCS_RECORD_NAME['github_clone'] }")
        if github_release_list:
            self.save_str_to_gcs_ascii(
                bucket=self.bucket,
                string_obj="\n".join(github_release_list),
                filename=f"{ folder }/{ GCS_RECORD_NAME['github_release'] }")

    def archive_dockerhub_data(self, folder):
        dockerhub_image_list = list()
        for image, pull_count in self.dockerhub_stats.items():
            dockerhub_image_record = dict(
                image=image,
                date=str(self.get_yesterday()),
                pull_count=pull_count)
            dockerhub_image_list.append(f"{ json.dumps(dockerhub_image_record) }")
        if dockerhub_image_list:
            self.save_str_to_gcs_ascii(
                bucket=self.bucket,
                string_obj="\n".join(dockerhub_image_list),
                filename=f"{ folder }/{ GCS_RECORD_NAME['dockerhub_image'] }")

    def archive_data(self):
        """
        Archive Data to GCS Bucket
        """
        # records/2021-04-21
        folder = f"records/{ datetime.datetime.now().date() }"
        self.archive_github_data(folder)
        self.archive_dockerhub_data(folder)
        return folder

    def load_bigquery_from_gcs(self, bq_client, gcs_uri, table_id, job_config):
        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_id,
            location=GCP_LOCATION,  # Must match the destination dataset location.
            job_config=job_config,
        )  # Make an API request.
        return load_job

    def load_data(self, record_folder):
        """
        load files like:
            gs://nebula-insights/records/2021-04-21/github_release_stats.json
            gs://nebula-insights/records/2021-04-21/github_clone_stats.json
            gs://nebula-insights/records/2021-04-21/dockerhub_image_stats.json
        """
        self.get_sink_credential()

        # Construct a BigQuery client object.
        bq_client = bigquery.Client()
        TABLE_ID_PREFIX = f"{ GCP_PROJECT }.{ BQ_DATASET }"
        URI_PREFIX = f"gs://{ BUCKET }/{ record_folder }"

        # table_id
        github_clone_table_id = f"{ TABLE_ID_PREFIX }.{ BQ_TABLE_NAME['github_clone'] }"
        github_release_table_id = f"{ TABLE_ID_PREFIX }.{ BQ_TABLE_NAME['github_release'] }"
        dockerhub_image_table_id = f"{ TABLE_ID_PREFIX }.{ BQ_TABLE_NAME['dockerhub_image'] }"

        # job_config
        github_clone_job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("repo", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("count", "INTEGER", mode="REQUIRED")
            ],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        # nested, referring to 
        # https://googleapis.dev/python/bigquery/latest/generated/\
        #         google.cloud.bigquery.schema.SchemaField.html
        github_release_job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("repo", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("count", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("tag", "STRING", mode="REQUIRED"),
                bigquery.SchemaField(
                    "assets", "RECORD", mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
                        bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
                        bigquery.SchemaField("count", "INTEGER", mode="REQUIRED"),
                        ])
            ],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        dockerhub_image_job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("image", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("pull_count", "INTEGER", mode="REQUIRED")
            ],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        # uri
        github_clone_uri = f"{ URI_PREFIX }/{ GCS_RECORD_NAME['github_clone'] }"
        github_release_uri = f"{ URI_PREFIX }/{ GCS_RECORD_NAME['github_release'] }"
        dockerhub_image_uri = f"{ URI_PREFIX }/{ GCS_RECORD_NAME['dockerhub_image'] }"

        self.load_bigquery_from_gcs(
            bq_client, github_clone_uri, github_clone_table_id, github_clone_job_config)

        self.load_bigquery_from_gcs(
            bq_client, github_release_uri, github_release_table_id, github_release_job_config)

        self.load_bigquery_from_gcs(
            bq_client, dockerhub_image_uri, dockerhub_image_table_id, dockerhub_image_job_config)


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

    dafa_fetcher.load_data(record_folder)


class DockerHubClient:
    """ Wrapper to communicate with docker hub API """
    def __init__(self, auth_token=None):
        self.config = {'auth_token': auth_token}
        self.auth_token = self.config.get('auth_token')

    def do_request(self, url, method='GET', data={}):
        valid_methods = ['GET', 'POST']
        if method not in valid_methods:
            raise ValueError('Invalid HTTP request method')
        headers = {'Content-type': 'application/json'}
        if self.auth_token:
            headers['Authorization'] = 'JWT ' + self.auth_token
        request_method = getattr(requests, method.lower())
        if len(data) > 0:
            data = json.dumps(data, indent=2, sort_keys=True)
            resp = request_method(url, data, headers=headers)
        else:
            resp = request_method(url, headers=headers)
        content = {}
        if resp.status_code == 200:
            content = json.loads(resp.content.decode())
        return {'content': content, 'code': resp.status_code}

    def login(self, username=None, password=None, save_config=True):
        data = {'username': username, 'password': password}
        self.auth_token = None
        resp = self.do_request(DOCKER_HUB_API_ENDPOINT + 'users/login/',
                               'POST', data)
        if resp['code'] == 200:
            self.auth_token = resp['content']['token']
            if save_config:
                self.config.set('auth_token', self.auth_token)
        return resp['code'] == 200

    def get_token(self):
        return self.auth_token

    def get_repos(self, org, page=1, per_page=PER_PAGE):
        url = '{0}repositories/{1}/?page={2}&page_size={3}'. \
               format(DOCKER_HUB_API_ENDPOINT, org, page, per_page)
        return self.do_request(url)

    def get_tags(self, org, repo, page=1, per_page=PER_PAGE):
        url = '{0}repositories/{1}/{2}/tags?page={3}&page_size={4}'. \
               format(DOCKER_HUB_API_ENDPOINT, org, repo, page, per_page)
        return self.do_request(url)

    def get_users(self, username):
        url = '{0}users/{1}'.format(DOCKER_HUB_API_ENDPOINT, username)
        return self.do_request(url)

    def get_buildhistory(self, org, repo, page=1, per_page=PER_PAGE):
        url = '{0}repositories/{1}/{2}/buildhistory?page={3}&page_size={4}'. \
                format(DOCKER_HUB_API_ENDPOINT, org, repo, page, per_page)
        return self.do_request(url)
