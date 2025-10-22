import os
import logging as log
from typing import Iterable, List, Mapping, Optional

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account


def _get_bq_client(credentials_file: Optional[str], project_id: Optional[str]) -> bigquery.Client:
    """Create a BigQuery client using a service account file if provided.

    """
    if credentials_file and os.path.isfile(credentials_file):
        log.info("BQ auth: using service account file at %s", credentials_file)
        creds = service_account.Credentials.from_service_account_file(credentials_file)
        project = project_id or getattr(creds, "project_id", None)
        return bigquery.Client(project=project, credentials=creds)
    # Fallback to application default credentials (e.g., GOOGLE_APPLICATION_CREDENTIALS)
    log.info("BQ auth: using application default credentials; project=%s", project_id)
    return bigquery.Client(project=project_id)


def _ensure_table(client: bigquery.Client, dataset: str, table: str) -> bigquery.Table:
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    try:
        # If table has no schema, we update the schema
        existing = client.get_table(table_ref)
        log.info("BQ table exists: %s.%s.%s (fields=%d)", client.project, dataset, table, len(existing.schema))

        existing_fields = {f.name for f in existing.schema}
        desired_optional = [
            bigquery.SchemaField("Date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("Ad_Account", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Country", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Campaign_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Campaign_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Adgroup_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Adgroup_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Spend", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("Currency", "STRING", mode="NULLABLE"),
        ]
        to_add: List[bigquery.SchemaField] = [f for f in desired_optional if f.name not in existing_fields]
        if to_add:
            new_schema = list(existing.schema) + to_add
            existing.schema = new_schema
            existing = client.update_table(existing, ["schema"])
            log.info("BQ table schema extended: %s.%s.%s (+%d fields)", client.project, dataset, table, len(to_add))
        return existing
    except NotFound:
        schema = [
            bigquery.SchemaField("Date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("Ad_Account", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Country", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Spend", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("Campaign_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Campaign_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Adgroup_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Adgroup_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Currency", "STRING", mode="NULLABLE"),
        ]
        table_obj = bigquery.Table(table_ref, schema=schema)
        created = client.create_table(table_obj)
        log.info("BQ table created: %s.%s.%s", client.project, dataset, table)
        return created


def write_rows_to_bigquery(
    rows: Iterable[Mapping[str, object]],
    *,
    dataset: str,
    table: str,
    project_id: Optional[str] = None,
    credentials_file: Optional[str] = None,
    batch_size: int = 500,
) -> None:
    """Write rows to BigQuery table, creating it if it doesn't exist.
    """
    client = _get_bq_client(credentials_file, project_id)
    _ensure_table(client, dataset, table)

    buffer: List[Mapping[str, object]] = []
    total_inserted = 0
    for row in rows:
        buffer.append(row)
        if len(buffer) >= batch_size:
            log.info("BQ insert batch: table=%s.%s.%s size=%d", client.project, dataset, table, len(buffer))
            errors = client.insert_rows_json(f"{client.project}.{dataset}.{table}", buffer)
            if errors:
                # Raise the first error with context
                raise RuntimeError(f"BigQuery insert failed: {errors}")
            total_inserted += len(buffer)
            buffer.clear()

    if buffer:
        log.info("BQ insert final batch: table=%s.%s.%s size=%d", client.project, dataset, table, len(buffer))
        errors = client.insert_rows_json(f"{client.project}.{dataset}.{table}", buffer)
        if errors:
            raise RuntimeError(f"BigQuery insert failed: {errors}")
        total_inserted += len(buffer)
    log.info("BQ insert done: table=%s.%s.%s total_rows=%d", client.project, dataset, table, total_inserted)


