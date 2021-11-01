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
    "nebula-clients",
    "nebula-gears",
    ".github"
]

MERGED_TEMPLATE = "repo:{org}/{repo} is:pr merged:{left}..{right}"

CREATED_OPEN_TEMPLATE = "repo:{org}/{repo} is:issue is:open created:{left}..{right}"
CREATED_CLOSED_TEMPLATE = "repo:{org}/{repo} is:issue is:closed closed:{left}..{right}"

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
        self.open_issues = {}
        self.closed_issues = {}
        self.report_body = []
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

    def get_data(self, left=None, right=None):
        """from github API, dockerhub API, etc."""

        print(f"[INFO] { datetime.datetime.now() } "
              f"Started fetching data from github")
        if left is None:
            left = str(self.get_yesterday())
            right = str(datetime.datetime.now().date())
        self.get_data_from_github(left, right)

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
            if repo.private:
                continue
            repo_key = f"{ org.login }/{ repo.name }"
            if repo.name == REPORT_REPO:
                self.report_repo = repo
            if repo.name not in GH_REPO_EXCLUDE_LIST:
                self.get_github_contributors(g, org_str, repo, left, right,
                    excluded_members=self.team_members)
                self.get_issues(g, org_str, repo, left, right)
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
                if timeIndex and timeIndex >= datetime.datetime.strptime(
                        left, '%Y-%m-%d') \
                        and timeIndex < datetime.datetime.strptime(
                            right, '%Y-%m-%d'):
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
                sleep_time = self.get_github_sleep_time(gh)
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

    def get_issues(self, gh, orgName, repo, left, right):
        open_issue = iter(gh.search_issues(CREATED_OPEN_TEMPLATE.format(
                org=orgName,
                repo=repo.name,
                left=left,
                right=right),
            "created", "desc"))
        closed_issue = iter(gh.search_issues(CREATED_CLOSED_TEMPLATE.format(
                org=orgName,
                repo=repo.name,
                left=left,
                right=right),
            "created", "desc"))

        timeIndex = None
        open_issues = []
        closed_issues = []
        while True:
            try:
                issue = next(open_issue)
                if DEBUG:
                    print(f"[DEBUG] issue fetched: {issue}")
                issue_record = {
                    "title": issue.title,
                    "user": issue.user.login,
                    "number": issue.number,
                    "created_at": str(issue.created_at),
                    "closed_at": "",
                    "closed_by": ""
                }
                open_issues.append(issue_record)
            except StopIteration:
                break  # loop end
            except RateLimitExceededException:
                sleep_time = get_sleep_time(gh)
                if DEBUG:
                    print(f"[DEBUG] RateLimitExceeded, sleep {sleep_time} sec")
                time.sleep(sleep_time)
                continue

        while True:
            try:
                issue = next(closed_issue)
                if DEBUG:
                    print(f"[DEBUG] issue fetched: {issue}")
                issue_record = {
                    "title": issue.title,
                    "user": issue.user.login,
                    "number": issue.number,
                    "created_at": str(issue.created_at),
                    "closed_at": str(issue.closed_at),
                    "closed_by": issue.closed_by.login
                }
                closed_issues.append(issue_record)
            except StopIteration:
                break  # loop end
            except RateLimitExceededException:
                sleep_time = get_sleep_time(gh)
                if DEBUG:
                    print(f"[DEBUG] RateLimitExceeded, sleep {sleep_time} sec")
                time.sleep(sleep_time)
                continue
        self.open_issues[repo.name] = open_issues
        self.closed_issues[repo.name] = closed_issues

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

    def get_lastweekday(self):
        return (datetime.datetime.now() - datetime.timedelta(7)).date()

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

    def send_issue(self, right=datetime.datetime.now().date()):
        if self.report_repo:
            label = self.report_repo.get_label("weekly report")
            self.report_repo.create_issue(
                title=f"Weekly Report {right}",
                body=self.report_body,
                labels=[label])
        else:
            print("[WARN] report repo was not fetched...")

    def generate_report(self):
        """
        Generation of .md report
        """

        # Weekly Report of Nebula Graph Community

        body = []
        body.append("# Weekly Report of Nebula Graph Community\n")

        ## Contributors # tbd: diff_hunk https://github.com/PyGithub/PyGithub/blob/babcbcd04fd5605634855f621b8558afc5cbc515/github/PullRequestComment.py
        contributions_headers = (
            "| Repo | Author | PR   |\n"
            "| ---- | ------ | ---- |"
            )
        body.append("## Contributions Summary\n")
        ### New Contributors
        all_new_contributors = [
            (repo, author, f"[#{issue.split('/')[-1]}]({issue})")
            for repo, repo_dict in self.new_contributors.items()
            for author, issues in repo_dict.items()
            for issue in list(issues)
        ]
        if all_new_contributors:
            body.append("### New Contributors\n")
            body.append("> Note, new contributor means first merged PR in its repo.\n")
            # TBD, add count
            body.append(contributions_headers)
            for row in all_new_contributors:
                body.append(f"| {'| '.join(row)} |")
            body.append("\n")
            body.append(
                f"> Total New Contributors' PR of the Week: "
                f"`{ len(all_new_contributors) }`\n")

        ### All nForce Contributors
        all_nforce_contributors = [
            (repo, author, f"[#{issue.split('/')[-1]}]({issue})")
            for repo, repo_dict in self.internal_contributors.items()
            for author, issues in repo_dict.items()
            for issue in list(issues)
        ]

        if all_nforce_contributors:
            body.append("### nForce Contributions\n")
            # TBD, add count
            body.append(contributions_headers)
            for row in all_nforce_contributors:
                body.append(f"| {'| '.join(row)} |")
            body.append("\n")
            body.append(
                f"> Total nForce Contributors' PR of the Week: "
                f"`{ len(all_nforce_contributors) }`\n")

        ### All community Contributors
        all_external_contributors = [
            (repo, author, f"[#{issue.split('/')[-1]}]({issue})")
            for repo, repo_dict in self.all_external_contributors.items()
            for author, issues in repo_dict.items()
            for issue in list(issues)
        ]

        if all_external_contributors:
            body.append("### Community Contributions\n")
            # TBD, add count
            body.append(contributions_headers)
            for row in all_external_contributors:
                body.append(f"| {'| '.join(row)} |")
            body.append("\n")
            body.append(
                f"> Total Community Contributors' PR of the Week: "
                f"`{ len(all_external_contributors) }`\n")

        if not any((
                all_new_contributors,
                all_nforce_contributors,
                all_external_contributors)):
            body.append("> There is no contributions...\n")

        ## ISSUE Summary

        body.append("## Issue Summary")

        ### Open Issues of the Week
        open_issue_headers = (
            "| Repo | Author | ISSUE | Created At   |\n"
            "| ---- | ------ | ----- | ------------ |"
            )
        if any(self.open_issues.values()):
            body.append("### Open Issues of the Week\n")
            # TBD, add count
            body.append(open_issue_headers)
            for repo, issues in self.open_issues.items():
                url = f"https://github.com/{GH_ORG}/{repo}/issues/"
                for issue in issues:
                    body.append(
                        f"| {repo} | {issue['user']} | "
                        f"[#{issue['number']}]({url}{issue['number']}) | "
                        f"{issue['created_at']} |")
            body.append("\n")
            body.append(
                f"> Total Open issues of the Week: "
                f"`{ len(self.open_issues.items()) }`\n")
        else:
            body.append("> There is no open issues...\n")

        ### Closed Issues of the Week
        closed_issue_headers = (
            "| Repo | Author | ISSUE | Created At   | Closed At   |\n"
            "| ---- | ------ | ----- | ------------ | ------------ |"
            )
        if any(self.closed_issues.values()):
            body.append("### Closed Issues of the Week\n")
            # TBD, add count
            body.append(closed_issue_headers)
            for repo, issues in self.closed_issues.items():
                url = f"https://github.com/{GH_ORG}/{repo}/issues/"
                for issue in issues:
                    body.append(
                        f"| {repo} | {issue['user']} | "
                        f"[#{issue['number']}]({url}{issue['number']}) | "
                        f"{issue['created_at']} | "
                        f"{issue['closed_at']} |")
            body.append("\n")
            body.append(
                f"> Total Closed issues of the Week: "
                f"`{ len(self.closed_issues.items()) }`\n")
        else:
            body.append("> There is no closed issues...\n")

        # This should not be published
        # ########### BigQuery Started
        # bq_client = bigquery.Client()
        # left = self.get_lastweekday()
        # right = self.get_yesterday()
        # #--------------------- async calling -------------------

        # ## Release Stats
        # github_release_query = (
        #     f"DECLARE start_date, end_date DATE;"
        #     f"SET start_date = DATE({left.year}, {left.month}, {left.day});"
        #     f"SET end_date = DATE({right.year}, {right.month}, {right.day});"
        #     f"SELECT repo, date, tag, count "
        #     f"FROM `nebula-insights.nebula_insights.github_release_records` "
        #     f"WHERE date in (start_date, end_date);"
        #     )
        # github_release_query_job = bq_client.query(github_release_query)

        # ## Clone Stats
        # github_clone_query = (
        #     f"DECLARE start_date, end_date DATE;"
        #     f"SET start_date = DATE({left.year}, {left.month}, {left.day});"
        #     f"SET end_date = DATE({right.year}, {right.month}, {right.day});"
        #     f"SELECT * "
        #     f"FROM `nebula-insights.nebula_insights.github_clone_records` "
        #     f"WHERE date between start_date and end_date;"
        #     )
        # github_clone_query_job = bq_client.query(github_clone_query)

        # ## Docker Hub Stats

        # dockerhub_image_query = (
        #     f"DECLARE start_date, end_date DATE;"
        #     f"SET start_date = DATE({left.year}, {left.month}, {left.day});"
        #     f"SET end_date = DATE({right.year}, {right.month}, {right.day});"
        #     f"SELECT * "
        #     f"FROM `nebula-insights.nebula_insights.dockerhub_image_records` "
        #     f"WHERE date in (start_date, end_date);"
        #     )
        # dockerhub_image_query_job = bq_client.query(dockerhub_image_query)

        # #--------------------- async calling -------------------
        # ## Release Stats
        # release_headers = (
        #     f"| Repo | Tag  | {left} | {right} | Incrementation |\n"
        #     f"| ---- | ---  | ------ | ------- | -------------- |"
        #     )
        # body.append("## Release Assets Download Statistics of the Week\n")
        # body.append(release_headers)
        # release_data = {}
        # release_sum = 0

        # for row in github_release_query_job.result():
        #     if row.repo in GH_REPO_EXCLUDE_LIST:
        #         continue
        #     default_tag = {
        #             row.tag: {
        #                 str(left): 0,
        #                 str(right): 0
        #             }
        #         }
        #     if row.repo not in release_data:
        #         release_data[row.repo] = dict(default_tag)
        #     if row.tag not in release_data[row.repo]:
        #         release_data[row.repo].update(default_tag)
        #     release_data[row.repo][row.tag][str(row.date)] = row.count
        # for repo, repo_value in release_data.items():
        #     for tag, count in repo_value.items():
        #         if count[str(right)] - count[str(left)] <= 0:
        #             continue
        #         release_sum += count[str(right)] - count[str(left)]
        #         body.append(
        #             f"| {repo} | {tag}| "
        #             f"{count[str(left)]} | {count[str(right)]}| "
        #             f"{count[str(right)] - count[str(left)]}|")
        # body.append("\n")
        # body.append(
        #     f"> Github Release Assents Download Count of the Week: "
        #     f"`{ release_sum }`\n")

        # ## Clone Stats
        # dates = list(
        #     (str(left + datetime.timedelta(delta)) for delta in range(7)))
        # date_headers = "|".join(dates)
        # clone_headers = (
        #     f"| Repo | { date_headers }          | SUM |\n"
        #     f"| ---- | { '-|'*(len(dates)-1) } - | -------------- |"
        #     )
        # body.append("## Clone Statistics of the Week\n")
        # body.append(clone_headers)
        # clone_data = {}
        # clone_sum = 0

        # for row in github_clone_query_job.result():
        #     if row.repo in GH_REPO_EXCLUDE_LIST:
        #         continue
        #     if row.repo not in clone_data:
        #         clone_data[row.repo] = dict((d, 0) for d in dates)
        #     clone_data[row.repo][str(row.date)] = row.count

        # for repo, count in clone_data.items():
        #     count_values = "|".join(
        #         (str(count[d]) for d in dates))
        #     if sum(count.values()) == 0:
        #         continue
        #     clone_sum += sum(count.values())
        #     body.append(
        #         f"| {repo} | "
        #         f"{ count_values } | "
        #         f"{sum(count.values())}|")
        # body.append("\n")
        # body.append(
        #     f"> Github Clone Count of the Week: "
        #     f"`{ clone_sum }`\n")

        # ## Docker Hub Stats
        # docker_headers = (
        #     f"| Repo | {left} | {right} | Incrementation |\n"
        #     f"| ---- | ------ | ------- | -------------- |"
        #     )
        # body.append("## Docker Hub Image Pull Count Statistics of the Week\n")
        # body.append(docker_headers)
        # docker_data = {}
        # docker_sum = 0
        # for row in dockerhub_image_query_job.result():
        #     if row.image in GH_REPO_EXCLUDE_LIST:
        #         continue
        #     default_value = {str(left): 0, str(right): 0}
        #     if row.image not in docker_data:
        #         docker_data[row.image] = dict(default_value)
        #     docker_data[row.image][str(row.date)] = row.pull_count
        # for repo, count in docker_data.items():
        #     if count[str(right)] - count[str(left)] <= 0:
        #         continue
        #     docker_sum += count[str(right)] - count[str(left)]
        #     body.append(
        #         f"| {repo} | "
        #         f"{count[str(left)]} | {count[str(right)]}| "
        #         f"{count[str(right)] - count[str(left)]}|")
        # body.append("\n")
        # body.append(
        #     f"> Docker Hub Image Pull Count of the Week: "
        #     f"`{ docker_sum }`\n")

        ## Docker Hub Stats end

        ########### BigQuery Ended
        if DEBUG:
            print("\n".join(body))
        self.report_body = "\n".join(body)

def data_fetch(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    # debug in CLI:
    # DATA=$(printf '{"report_true": "True", "report_left": "2021-09-19", "report_right": "2021-09-25", "report_true": "True"}' | base64)
    # gcloud functions call the-function --data '{"data":"'$DATA'"}'

    weekly_report = DataFetcher()
    send_report = datetime.date.today().weekday() == 5
    left = str(weekly_report.get_lastweekday())
    right = str(weekly_report.get_yesterday())
    if 'data' in event:
        decoded_data = base64.b64decode(event['data']).decode('utf-8')
        if decoded_data and isinstance(json.loads(decoded_data), dict):
            payload = json.loads(decoded_data)
            left = payload.get("report_left", left)
            right = payload.get("report_right", right)
            send_report = payload.get("report_true", False) or send_report
            global DEBUG
            DEBUG = bool(payload.get("debug_true", DEBUG))

    # report
    if send_report:
        weekly_report.get_data(left=left, right=right)
        weekly_report.generate_report()
        weekly_report.send_issue(right=right)

    # daily data
    data_fetcher = DataFetcher()
    data_fetcher.get_data()
    record_folder = data_fetcher.archive_data()
