from pathlib import Path

from src.audit.audit_manager import AuditManager


def test_quality_rule_is_persisted(
    tmp_path: Path,
) -> None:
    audit = AuditManager(
        tmp_path
        / "audit"
    )

    pipeline_id = (
        audit.new_pipeline_id()
    )

    stage_run_id = (
        audit.new_stage_run_id(
            "QUALITY"
        )
    )

    status = (
        audit.record_quality_rule(
            pipeline_id=pipeline_id,
            stage_name="QUALITY",
            stage_run_id=stage_run_id,
            dataset_name=(
                "silver_cleaned_yellow_2026"
            ),
            rule_id=(
                "DQ_YELLOW_2026_COMPLETITUD"
            ),
            rule_name=(
                "Completitud de yellow 2026"
            ),
            records_evaluated=100,
            records_failed=5,
            allowed_failure_rate=0.10,
            metadata={
                "score": 0.95,
                "threshold": 0.90,
            },
        )
    )

    assert status == "PASSED"

    outputs = (
        audit.export_parquet_views()
    )

    assert (
        "quality_results"
        in outputs
    )
