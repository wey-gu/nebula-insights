> Manualy test functions

Modify code:
```python
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
        # self.team_members = set()
        # self.s_client = storage.Client()
        self.bucket = self.s_client.get_bucket(BUCKET)
...
    def parse_conf(self):
        """conf for data fetching policies config or fetching API credentials
        ideally it's a file stored in Google Cloud Storage
        """
        # blob = self.bucket.blob('conf/config.json')
        # data = json.loads(blob.download_as_string(client=None))
        # self.conf.update(data)
        pass
```

> Then we could do as follow:

```python
weekly_report = DataFetcher()
weekly_report.conf = {
    "github_token":"",
    "team_id":""
}
weekly_report.get_data(
            left=str(weekly_report.get_lastweekday()),
            right=str(weekly_report.get_yesterday()))
weekly_report.generate_report()
weekly_report.send_issue()

## Test BigQuery

# Referring to https://cloud.google.com/bigquery/docs/authentication/end-user-installed

########### BigQuery

from google_auth_oauthlib import flow

appflow = flow.InstalledAppFlow.from_client_secrets_file(
    "client_secrets.json", scopes=["https://www.googleapis.com/auth/bigquery"])

appflow.run_console()
credentials = appflow.credentials
bq_client = bigquery.Client(project=GCP_PROJECT, credentials=credentials)

########### BigQuery

left = datetime.date(2021, 6, 20)
right = datetime.date(2021, 6, 25)


body_str="\n".join(body)

weekly_report.report_repo.create_issue(
    title=f"[test] Weekly Report {datetime.datetime.now().date()}",
    body=body_str)
```