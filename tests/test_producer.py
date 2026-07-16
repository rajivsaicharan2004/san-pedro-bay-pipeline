from unittest.mock import patch

from producer import DEADLETTER, envelope, handle_message, route


def test_envelope_wraps_payload_with_metadata():
    payload = {"MessageType": "PositionReport"}
    env = envelope(payload)
    assert env["payload"] == payload
    assert env["source"] == "aisstream"
    assert env["schema_version"] == 1
    assert "ingested_at" in env


def test_route_position_report():
    msg = {"MessageType": "PositionReport", "MetaData": {"MMSI": 123456789}}
    topic, key = route(msg)
    assert topic == "ais.raw.positions"
    assert key == "123456789"


def test_route_ship_static_data():
    msg = {"MessageType": "ShipStaticData", "MetaData": {"MMSI": 987654321}}
    topic, key = route(msg)
    assert topic == "ais.raw.static"
    assert key == "987654321"


def test_route_unknown_type_goes_to_deadletter():
    msg = {"MessageType": "SomethingElse", "MetaData": {"MMSI": 111222333}}
    topic, key = route(msg)
    assert topic == DEADLETTER
    assert key == "111222333"


def test_route_missing_mmsi_gives_none_key():
    msg = {"MessageType": "PositionReport", "MetaData": {}}
    topic, key = route(msg)
    assert topic == "ais.raw.positions"
    assert key is None


@patch("producer.producer")
def test_handle_message_malformed_json_goes_to_deadletter(mock_producer):
    handle_message("not valid json{")

    mock_producer.produce.assert_called_once()
    args, kwargs = mock_producer.produce.call_args
    assert args[0] == DEADLETTER
    # SerializingProducer.produce() takes a raw object -- the value
    # serializer (JSON for deadletter, Avro otherwise) runs inside
    # produce() itself, which is mocked out here, so kwargs["value"] is
    # still the plain dict, not yet encoded.
    body = kwargs["value"]
    assert body["raw"] == "not valid json{"
    assert "error" in body
