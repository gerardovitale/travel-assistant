import io
import os
import tempfile
from unittest import TestCase
from unittest.mock import MagicMock

import pandas as pd
from pipeline.base import PipelineResult
from pipeline.base import TaskConfig
from pipeline.gcs import CallableSink
from pipeline.gcs import CallableSource
from pipeline.gcs import IncrementalGCSParquetSink
from pipeline.runner import TaskRunner
from pipeline.runner import write_step_summary


def _make_ok_task(name="test_pipeline", rows=5):
    df = pd.DataFrame({"value": range(rows)})
    return TaskConfig(
        name=name,
        description="Test pipeline",
        output_blob=f"aggregates/{name}.parquet",
        source=CallableSource(lambda: df.copy()),
        transformations=[],
        sink=CallableSink(lambda _: None),
    )


def _make_failing_task(name="failing_pipeline"):
    def _raise():
        raise RuntimeError("intentional failure")

    return TaskConfig(
        name=name,
        description="A pipeline that always fails",
        output_blob=f"aggregates/{name}.parquet",
        source=CallableSource(_raise),
        transformations=[],
        sink=CallableSink(lambda _: None),
    )


class TestPipelineRunner(TestCase):

    def test_run_returns_results_for_each_pipeline(self):
        pipelines = [_make_ok_task("a"), _make_ok_task("b")]
        results = TaskRunner().run(pipelines)
        self.assertEqual(len(results), 2)

    def test_successful_pipeline_has_ok_status(self):
        result = TaskRunner().run([_make_ok_task(rows=3)])[0]
        self.assertEqual(result.status, "ok")
        self.assertIsNone(result.error)

    def test_successful_pipeline_records_row_counts(self):
        result = TaskRunner().run([_make_ok_task(rows=7)])[0]
        self.assertEqual(result.input_rows, 7)
        self.assertEqual(result.output_rows, 7)

    def test_successful_pipeline_records_positive_duration(self):
        result = TaskRunner().run([_make_ok_task()])[0]
        self.assertGreaterEqual(result.duration_seconds, 0.0)

    def test_failing_pipeline_has_failed_status(self):
        result = TaskRunner().run([_make_failing_task()])[0]
        self.assertEqual(result.status, "failed")
        self.assertIsNotNone(result.error)
        self.assertIn("intentional failure", result.error)

    def test_failing_pipeline_does_not_stop_subsequent_pipelines(self):
        pipelines = [_make_failing_task("fail"), _make_ok_task("after_fail")]
        results = TaskRunner().run(pipelines)
        self.assertEqual(results[0].status, "failed")
        self.assertEqual(results[1].status, "ok")

    def test_transform_chain_is_applied(self):
        def double(d):
            return d.assign(x=d["x"] * 2)

        def add_one(d):
            return d.assign(x=d["x"] + 1)

        df = pd.DataFrame({"x": [1, 2, 3]})

        sink_received = []
        task = TaskConfig(
            name="transform_test",
            description="Transform chain test",
            output_blob="aggregates/transform_test.parquet",
            source=CallableSource(lambda: df.copy()),
            transformations=[double, add_one],
            sink=CallableSink(lambda d: sink_received.append(d["x"].tolist())),
        )
        TaskRunner().run([task])
        self.assertEqual(sink_received[0], [3, 5, 7])

    def test_pipeline_result_carries_name_and_blob(self):
        p = _make_ok_task(name="my_pipeline")
        result = TaskRunner().run([p])[0]
        self.assertEqual(result.name, "my_pipeline")
        self.assertEqual(result.output_blob, "aggregates/my_pipeline.parquet")


class TestWriteStepSummary(TestCase):

    def test_writes_nothing_when_env_var_absent(self):
        env_backup = os.environ.pop("GITHUB_STEP_SUMMARY", None)
        try:
            results = [
                PipelineResult(
                    name="p",
                    description="d",
                    output_blob="b",
                    input_rows=1,
                    output_rows=1,
                    duration_seconds=0.1,
                    status="ok",
                )
            ]
            write_step_summary(results)  # should not raise
        finally:
            if env_backup is not None:
                os.environ["GITHUB_STEP_SUMMARY"] = env_backup

    def test_writes_markdown_table_to_summary_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
            summary_path = f.name

        try:
            os.environ["GITHUB_STEP_SUMMARY"] = summary_path
            results = [
                PipelineResult(
                    name="province_daily_stats",
                    description="Province aggregation",
                    output_blob="aggregates/province_daily_stats.parquet",
                    input_rows=12345,
                    output_rows=42,
                    duration_seconds=1.23,
                    status="ok",
                ),
                PipelineResult(
                    name="brand_win_rate",
                    description="Brand win rate",
                    output_blob="aggregates/reports/brand_win_rate.parquet",
                    input_rows=0,
                    output_rows=0,
                    duration_seconds=0.5,
                    status="failed",
                    error="some error",
                ),
            ]
            write_step_summary(results, title="Test Results")

            with open(summary_path, encoding="utf-8") as f:
                content = f.read()

            self.assertIn("## Test Results", content)
            self.assertIn("province_daily_stats", content)
            self.assertIn("brand_win_rate", content)
            self.assertIn("✅", content)
            self.assertIn("❌", content)
            self.assertIn("12,345", content)
            self.assertIn("```mermaid", content)
        finally:
            del os.environ["GITHUB_STEP_SUMMARY"]
            os.unlink(summary_path)

    def test_appends_to_existing_summary_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
            f.write("# Existing content\n")
            summary_path = f.name

        try:
            os.environ["GITHUB_STEP_SUMMARY"] = summary_path
            write_step_summary([], title="Appended")

            with open(summary_path, encoding="utf-8") as f:
                content = f.read()

            self.assertIn("# Existing content", content)
            self.assertIn("## Appended", content)
        finally:
            del os.environ["GITHUB_STEP_SUMMARY"]
            os.unlink(summary_path)


def _make_bucket_mock(existing_df=None):
    """Return a mock GCS bucket that serves `existing_df` on blob.download_as_bytes()."""
    bucket = MagicMock()
    blob = MagicMock()
    bucket.blob.return_value = blob
    if existing_df is None:
        blob.exists.return_value = False
    else:
        blob.exists.return_value = True
        blob.download_as_bytes.return_value = existing_df.to_parquet(index=False, compression="snappy")
    return bucket, blob


class TestIncrementalGCSParquetSink(TestCase):

    def test_writes_directly_when_no_existing_blob(self):
        df = pd.DataFrame({"date": ["2026-01-01"], "value": [10]})
        bucket, blob = _make_bucket_mock(existing_df=None)

        IncrementalGCSParquetSink(bucket, "test.parquet").write(df)

        blob.upload_from_string.assert_called_once()

    def test_merged_df_contains_existing_and_new_rows(self):
        existing = pd.DataFrame({"date": ["2026-01-01"], "value": [10]})
        new_day = pd.DataFrame({"date": ["2026-01-02"], "value": [20]})
        bucket, blob = _make_bucket_mock(existing_df=existing)

        captured = []

        def capture(data, *args, **kwargs):
            captured.append(pd.read_parquet(io.BytesIO(data)))

        blob.upload_from_string.side_effect = capture
        IncrementalGCSParquetSink(bucket, "test.parquet").write(new_day)

        self.assertEqual(len(captured), 1)
        self.assertEqual(len(captured[0]), 2)

    def test_deduplicates_same_date_on_rerun(self):
        existing = pd.DataFrame({"date": ["2026-01-01", "2026-01-02"], "value": [10, 20]})
        same_day = pd.DataFrame({"date": ["2026-01-02"], "value": [99]})
        bucket, blob = _make_bucket_mock(existing_df=existing)

        captured = []

        def capture(data, *args, **kwargs):
            captured.append(pd.read_parquet(io.BytesIO(data)))

        blob.upload_from_string.side_effect = capture
        IncrementalGCSParquetSink(bucket, "test.parquet").write(same_day)

        result = captured[0]
        self.assertEqual(len(result), 2)
        self.assertEqual(result[result["date"] == "2026-01-02"]["value"].iloc[0], 99)

    def test_prunes_rows_beyond_retention_days(self):
        existing = pd.DataFrame(
            {
                "date": ["2025-01-01", "2026-01-01"],
                "value": [10, 20],
            }
        )
        new_day = pd.DataFrame({"date": ["2026-01-02"], "value": [30]})
        bucket, blob = _make_bucket_mock(existing_df=existing)

        captured = []

        def capture(data, *args, **kwargs):
            captured.append(pd.read_parquet(io.BytesIO(data)))

        blob.upload_from_string.side_effect = capture
        IncrementalGCSParquetSink(bucket, "test.parquet", retention_days=365).write(new_day)

        result = captured[0]
        dates = set(result["date"].astype(str).tolist())
        self.assertNotIn("2025-01-01", dates)
        self.assertIn("2026-01-01", dates)
        self.assertIn("2026-01-02", dates)

    def test_noop_when_df_is_empty(self):
        bucket, blob = _make_bucket_mock(existing_df=None)
        IncrementalGCSParquetSink(bucket, "test.parquet").write(pd.DataFrame())
        blob.upload_from_string.assert_not_called()
