
Cheatsheets For BigQuery

```bash
❯ bq show --schema --format=prettyjson \
    nebula-insights:nebula_insights.github_clone_records \
        > ./github_clone_records_schema.json

❯ bq show --schema --format=prettyjson \
    nebula-insights:nebula_insights.github_release_records \
        > ./github_release_records_schema.json

❯ bq show --schema --format=prettyjson \
    nebula-insights:nebula_insights.dockerhub_image_records \
        > ./dockerhub_image_records_schema.json
```

```bash
❯ bq mk --table --description github_clone_records \
    nebula-insights:nebula_insights.github_clone_records \
        ./github_clone_records_schema.json

❯ bq mk --table --description github_release_records \
    nebula-insights:nebula_insights.github_release_records \
        ./github_release_records_schema.json

❯ bq mk --table --description dockerhub_image_records \
    nebula-insights:nebula_insights.dockerhub_image_records \
        ./dockerhub_image_records_schema.json

```

```bash
❯ bq rm --table nebula-insights:nebula_insights.github_clone_records
rm: remove table 'nebula-insights:nebula_insights.github_clone_records'? (y/N) y

❯ bq rm --table nebula-insights:nebula_insights.github_release_records
rm: remove table 'nebula-insights:nebula_insights.github_release_records'? (y/N) y

❯ bq rm --table nebula-insights:nebula_insights.dockerhub_image_records
rm: remove table 'nebula-insights:nebula_insights.dockerhub_image_records'? (y/N) y

```
