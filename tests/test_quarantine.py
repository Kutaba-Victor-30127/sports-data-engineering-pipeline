import json

from src.ingestion.load_events import load_events


def test_invalid_record_is_written_to_quarantine(
    monkeypatch,
):
    csv_content = b"""event_id,sport,league,home_team,away_team,event_time,home_odds,draw_odds,away_odds,source_updated_at
1,football,Premier League,Arsenal,Chelsea,2026-09-21T18:00:00Z,1.95,3.40,4.20,2026-10-26T10:00:00Z
2,football,La Liga,Barcelona,Valencia,2026-09-21T20:00:00Z,-1.50,4.10,6.50,2026-10-26T10:00:00Z
"""

    captured_upload = {}

    def fake_read_file_from_s3(s3_key):
        return csv_content

    def fake_upload_text_to_s3(
        content,
        s3_key,
        content_type="application/json",
    ):
        captured_upload["content"] = content
        captured_upload["s3_key"] = s3_key
        captured_upload["content_type"] = content_type

    monkeypatch.setattr(
        "src.ingestion.load_events.read_file_from_s3",
        fake_read_file_from_s3,
    )

    monkeypatch.setattr(
        "src.ingestion.load_events.upload_text_to_s3",
        fake_upload_text_to_s3,
    )

    source_key = (
        "raw/events/"
        "ingestion_date=2026-11-02/"
        "batch_id=test-batch/"
        "events.csv"
    )

    valid_df, rejected_df = load_events(
        s3_key=source_key,
        batch_id="test-batch",
    )

    assert len(valid_df) == 1
    assert len(rejected_df) == 1

    assert rejected_df.iloc[0]["event_id"] == 2

    assert (
        rejected_df.iloc[0]["rejection_reason"]
        == "home_odds must be positive"
    )

    assert captured_upload["s3_key"] == (
        "rejected/events/"
        "ingestion_date=2026-11-02/"
        "batch_id=test-batch/"
        "rejected.json"
    )

    quarantine_records = json.loads(
        captured_upload["content"]
    )

    assert len(quarantine_records) == 1
    assert quarantine_records[0]["event_id"] == 2
    assert quarantine_records[0]["batch_id"] == "test-batch"
    assert quarantine_records[0]["source_file"] == source_key

    assert (
        quarantine_records[0]["rejection_reason"]
        == "home_odds must be positive"
    )


def test_valid_batch_does_not_write_quarantine(
    monkeypatch,
):
    csv_content = b"""event_id,sport,league,home_team,away_team,event_time,home_odds,draw_odds,away_odds,source_updated_at
1,football,Premier League,Arsenal,Chelsea,2026-09-21T18:00:00Z,1.95,3.40,4.20,2026-10-26T10:00:00Z
2,football,La Liga,Barcelona,Valencia,2026-09-21T20:00:00Z,1.50,4.10,6.50,2026-10-26T10:00:00Z
"""

    def fake_read_file_from_s3(s3_key):
        return csv_content

    def fail_if_quarantine_is_called(*args, **kwargs):
        raise AssertionError(
            "Quarantine should not be written "
            "for a fully valid batch."
        )

    monkeypatch.setattr(
        "src.ingestion.load_events.read_file_from_s3",
        fake_read_file_from_s3,
    )

    monkeypatch.setattr(
        "src.ingestion.load_events.upload_text_to_s3",
        fail_if_quarantine_is_called,
    )

    valid_df, rejected_df = load_events(
        s3_key="raw/events/test/events.csv",
        batch_id="valid-test-batch",
    )

    assert len(valid_df) == 2
    assert rejected_df.empty