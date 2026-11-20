from collections import Counter
import json
from pathlib import Path

import pytest

from offline.common import config
from common.schemas import validate_message
from producer import wiki_kafka_producer as wp

FIXTURE = Path(__file__).parent / "fixtures" / "replay_sample.jsonl"


def _event(stream="mediawiki.revision-create", **overrides):
    event = {
        "meta": {"stream": stream, "domain": "en.wikipedia.org", "dt": "2026-01-05T08:00:01Z"},
        "database": "enwiki", "page_id": 42, "page_title": "Some_Page", "page_namespace": 0,
        "rev_id": 900, "rev_parent_id": 899, "rev_timestamp": "2026-01-05T08:00:00Z",
        "performer": {"user_text": "Alice", "user_is_bot": False},
    }
    event.update(overrides)
    return event


def _message(**overrides):
    message = {
        "article_id": 42, "revision_id": 900, "title": "Some Page", "raw_wikitext": "text",
        "event_type": "NEW", "event_time": "2026-01-05T08:00:00Z", "source": "replay",
    }
    message.update(overrides)
    return message


class FakeKafka:
    def __init__(self):
        self.produced = []

    def produce(self, topic, key, value, on_delivery):
        self.produced.append((topic, key, value))
        on_delivery(None, None)

    def poll(self, _timeout):
        return 0

    def flush(self, _timeout):
        return 0


# lọc sự kiện

def test_revision_create_with_parent_is_updated():
    assert wp.event_to_candidate(_event()) == {
        "article_id": 42, "revision_id": 900, "title": "Some Page",
        "event_type": "UPDATED", "event_time": "2026-01-05T08:00:00Z",
    }


def test_page_create_is_new():
    event = _event(stream="mediawiki.page-create")
    del event["rev_parent_id"]
    assert wp.event_to_candidate(event)["event_type"] == "NEW"


def test_first_revision_of_new_page_is_skipped_on_revision_create():
    event = _event()
    del event["rev_parent_id"]
    assert wp.event_to_candidate(event) is None


@pytest.mark.parametrize("overrides", [
    {"database": "viwiki"},
    {"page_namespace": 1},
    {"performer": {"user_text": "Bot", "user_is_bot": True}},
    {"meta": {"stream": "mediawiki.revision-create", "domain": "canary"}},
    {"meta": {"stream": "mediawiki.page-delete", "domain": "en.wikipedia.org"}},
    {"page_id": None},
])
def test_filtered_events(overrides):
    assert wp.event_to_candidate(_event(**overrides)) is None


def test_sse_parsing_handles_ids_comments_and_bad_json():
    lines = [
        ":ok", "", "event: message", 'id: [{"offset":1}]', 'data: {"a": 1}', "",
        "data: not json", "", 'id: [{"offset":2}]', 'data: {"b":', "data: 2}", "",
    ]
    assert list(wp.iter_sse_events(lines)) == [('[{"offset":1}]', {"a": 1}), ('[{"offset":2}]', {"b": 2})]


# API nội dung

def test_parse_revisions_response():
    payload = {"query": {"pages": [
        {"pageid": 42, "revisions": [
            {"revid": 900, "timestamp": "2026-01-05T08:00:00Z", "slots": {"main": {"content": "wikitext A"}}},
        ]},
        {"pageid": 43, "revisions": [{"revid": 901, "slots": {"main": {"texthidden": True}}}]},
        {"pageid": 44, "missing": True},
    ]}}
    assert wp.parse_revisions_response(payload) == {
        900: {"content": "wikitext A", "timestamp": "2026-01-05T08:00:00Z"},
    }
    assert wp.parse_revisions_response({}) == {}


def test_build_messages_skips_missing_revisions_and_validates():
    candidates = [
        {"article_id": 42, "revision_id": 900, "title": "A", "event_type": "UPDATED", "event_time": "2026-01-05T08:00:00Z"},
        {"article_id": 43, "revision_id": 901, "title": "B", "event_type": "NEW", "event_time": "2026-01-05T08:01:00Z"},
    ]
    messages = wp.build_messages(candidates, {900: {"content": "wikitext A", "timestamp": "x"}})
    assert [m["revision_id"] for m in messages] == [900]
    assert messages[0]["source"] == "eventstreams" and messages[0]["raw_wikitext"] == "wikitext A"
    assert validate_message(messages[0]) is None


class FakeResponse:
    def __init__(self, status, payload=None, headers=None):
        self.status_code, self._payload, self.headers = status, payload or {}, headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        return self.responses.pop(0)


def test_wiki_api_retries_with_backoff_and_sends_contact_user_agent():
    ok = {"query": {"pages": [{"revisions": [{"revid": 900, "timestamp": "t", "slots": {"main": {"content": "c"}}}]}]}}
    session = FakeSession([FakeResponse(503), FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200, ok)])
    sleeps = []
    api = wp.WikiApi(session, "contact@example.org", rate=1000.0, sleep=sleeps.append)
    assert api.fetch([900]) == {900: {"content": "c", "timestamp": "t"}}
    assert len(session.calls) == 3
    assert 1.0 in sleeps and 7.0 in sleeps
    assert "contact@example.org" in session.calls[0]["headers"]["User-Agent"]
    assert session.calls[0]["params"]["revids"] == "900"
    assert session.calls[0]["params"]["rvslots"] == "main"


def test_wiki_api_requires_contact_and_limits_batch():
    with pytest.raises(ValueError, match="WIKI_CONTACT"):
        wp.WikiApi(FakeSession([]), "  ")
    api = wp.WikiApi(FakeSession([]), "contact@example.org")
    with pytest.raises(ValueError):
        api.fetch(list(range(1, 52)))


# kiểm tra message

def test_valid_message():
    assert validate_message(_message()) is None


@pytest.mark.parametrize("overrides, expected", [
    ({"article_id": None}, "article_id"),
    ({"article_id": "42"}, "article_id"),
    ({"revision_id": 0}, "revision_id"),
    ({"revision_id": True}, "revision_id"),
    ({"title": "  "}, "title"),
    ({"raw_wikitext": 5}, "raw_wikitext"),
    ({"event_type": "DELETED"}, "event_type"),
    ({"event_time": "yesterday"}, "event_time"),
    ({"event_time": "2026-01-05T08:00:00"}, "event_time"),
])
def test_invalid_messages(overrides, expected):
    assert expected in validate_message(_message(**overrides))


def test_missing_field_and_non_object():
    message = _message()
    del message["raw_wikitext"]
    assert "raw_wikitext" in validate_message(message)
    assert validate_message([1, 2]) is not None


# publisher và replay

def test_publisher_routes_by_event_type_and_keys_by_article():
    kafka = FakeKafka()
    publisher = wp.Publisher(kafka)
    assert publisher.publish(_message(event_type="NEW"))
    assert publisher.publish(_message(event_type="UPDATED", revision_id=901))
    assert not publisher.publish(_message(event_type="BAD"))
    assert [(topic, key) for topic, key, _ in kafka.produced] == [
        (config.TOPIC_NEW, b"42"), (config.TOPIC_UPDATED, b"42"),
    ]
    assert json.loads(kafka.produced[0][2])["article_id"] == 42
    assert (publisher.sent, publisher.skipped, publisher.failed) == (2, 1, 0)


def test_publisher_skips_oversized_message(monkeypatch):
    monkeypatch.setattr(config, "KAFKA_MAX_MESSAGE_BYTES", 200)
    kafka = FakeKafka()
    publisher = wp.Publisher(kafka)
    assert not publisher.publish(_message(raw_wikitext="x" * 500))
    assert kafka.produced == [] and publisher.skipped == 1


def test_replay_skips_bad_lines(tmp_path):
    path = tmp_path / "replay.jsonl"
    path.write_text(
        json.dumps(_message()) + "\n\nnot json\n" + json.dumps(_message(event_type="UPDATED", revision_id=901, source="x")) + "\n",
        encoding="utf-8",
    )
    kafka = FakeKafka()
    publisher = wp.Publisher(kafka)
    wp.run_replay(path, publisher)
    assert [json.loads(value)["revision_id"] for _, _, value in kafka.produced] == [900, 901]
    assert all(json.loads(value)["source"] == "replay" for _, _, value in kafka.produced)


def test_replay_fixture_meets_requirements():
    messages = list(wp.read_replay(FIXTURE))
    assert all(validate_message(message) is None for message in messages)
    by_article = Counter(message["article_id"] for message in messages)
    assert 18 <= len(by_article) <= 25                       # khoảng 20 bài
    assert len({m["revision_id"] for m in messages}) == len(messages)

    # 1 bài NEW rồi 2 lần UPDATED liên tiếp, revision tăng dần.
    multi = [m for m in messages if by_article[m["article_id"]] == 3]
    assert [m["event_type"] for m in multi] == ["NEW", "UPDATED", "UPDATED"]
    assert [m["revision_id"] for m in multi] == sorted(m["revision_id"] for m in multi)

    texts = [m["raw_wikitext"] for m in messages]
    assert sum(text.lstrip().upper().startswith("#REDIRECT") for text in texts) == 1
    assert sum("{{disambiguation}}" in text.lower() for text in texts) == 1
    rich = [text for text in texts if "{|" in text and "{{Infobox" in text]
    assert len(rich) == 1 and "\n* " in rich[0]
    # Có bài nằm ngoài shard 0 để kiểm tra khoá shard.
    assert {config.shard_id(article_id) for article_id in by_article} >= {0, 1, 2}
