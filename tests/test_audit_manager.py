from pathlib import Path

import pandas as pd

from src.audit.audit_manager import AuditManager


def create_successful_pipeline(
    audit: AuditManager,
    pipeline_id: str,
    stage_name: str,
) -> None:
    audit.start_pipeline(
        pipeline_id,
        parameters={
            "years": [2026],
        },
    )

    stage_run_id = audit.start_stage(
        pipeline_id,
        stage_name,
        [
            "python",
            "example.py",
        ],
    )

    audit.finish_stage(
        pipeline_id,
        stage_name,
        stage_run_id,
        status="SUCCESS",
        duration_seconds=1.25,
        return_code=0,
    )

    audit.finish_pipeline(
        pipeline_id,
        status="SUCCESS",
    )


def test_historical_and_latest_views(
    tmp_path: Path,
) -> None:
    audit = AuditManager(
        tmp_path / "audit",
    )

    failed_pipeline_id = audit.new_pipeline_id()

    audit.start_pipeline(
        failed_pipeline_id,
    )

    failed_stage_id = audit.start_stage(
        failed_pipeline_id,
        "INGESTION",
        [
            "python",
            "ingestion.py",
        ],
    )

    audit.finish_stage(
        failed_pipeline_id,
        "INGESTION",
        failed_stage_id,
        status="FAILED",
        duration_seconds=2.0,
        return_code=1,
    )

    audit.finish_pipeline(
        failed_pipeline_id,
        status="FAILED",
        message="Fallo de prueba",
    )

    successful_pipeline_id = audit.new_pipeline_id()

    create_successful_pipeline(
        audit,
        successful_pipeline_id,
        "SILVER",
    )

    outputs = audit.export_parquet_views()

    pipeline_dataframe = pd.read_parquet(
        outputs["pipeline_runs"],
    )

    stage_dataframe = pd.read_parquet(
        outputs["stage_runs"],
    )

    latest_pipeline_dataframe = pd.read_parquet(
        outputs["latest_pipeline_run"],
    )

    latest_stage_dataframe = pd.read_parquet(
        outputs["latest_stage_runs"],
    )

    latest_successful_pipeline_dataframe = pd.read_parquet(
        outputs["latest_successful_pipeline"],
    )

    latest_successful_stage_dataframe = pd.read_parquet(
        outputs["latest_successful_stage_runs"],
    )

    assert (
        len(
            pipeline_dataframe,
        )
        == 2
    )

    assert (
        len(
            stage_dataframe,
        )
        == 2
    )

    assert latest_pipeline_dataframe.iloc[0]["pipeline_id"] == successful_pipeline_id

    assert latest_pipeline_dataframe.iloc[0]["status"] == "SUCCESS"

    assert (
        len(
            latest_stage_dataframe,
        )
        == 1
    )

    assert latest_stage_dataframe.iloc[0]["stage_name"] == "SILVER"

    assert latest_stage_dataframe.iloc[0]["status"] == "SUCCESS"

    assert (
        latest_successful_pipeline_dataframe.iloc[0]["pipeline_id"]
        == successful_pipeline_id
    )

    assert (
        latest_successful_stage_dataframe.iloc[0]["pipeline_id"]
        == successful_pipeline_id
    )


def test_latest_successful_pipeline_ignores_failure(
    tmp_path: Path,
) -> None:
    audit = AuditManager(
        tmp_path / "audit",
    )

    successful_pipeline_id = audit.new_pipeline_id()

    create_successful_pipeline(
        audit,
        successful_pipeline_id,
        "INGESTION",
    )

    failed_pipeline_id = audit.new_pipeline_id()

    audit.start_pipeline(
        failed_pipeline_id,
    )

    failed_stage_id = audit.start_stage(
        failed_pipeline_id,
        "SILVER",
        [
            "python",
            "silver.py",
        ],
    )

    audit.finish_stage(
        failed_pipeline_id,
        "SILVER",
        failed_stage_id,
        status="FAILED",
        duration_seconds=3.5,
        return_code=1,
    )

    audit.finish_pipeline(
        failed_pipeline_id,
        status="FAILED",
    )

    outputs = audit.export_parquet_views()

    latest_pipeline_dataframe = pd.read_parquet(
        outputs["latest_pipeline_run"],
    )

    latest_successful_pipeline_dataframe = pd.read_parquet(
        outputs["latest_successful_pipeline"],
    )

    assert latest_pipeline_dataframe.iloc[0]["pipeline_id"] == failed_pipeline_id

    assert latest_pipeline_dataframe.iloc[0]["status"] == "FAILED"

    assert (
        latest_successful_pipeline_dataframe.iloc[0]["pipeline_id"]
        == successful_pipeline_id
    )

    assert latest_successful_pipeline_dataframe.iloc[0]["status"] == "SUCCESS"
