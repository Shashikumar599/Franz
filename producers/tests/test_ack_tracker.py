from core.ack_tracker import AckTracker


def test_start_id_is_the_initial_watermark():
    assert AckTracker("x").watermark == "x"


def test_watermark_follows_acks_in_order():
    tracker = AckTracker()
    a, b = tracker.register("a"), tracker.register("b")
    tracker.ack(a)
    assert tracker.watermark == "a"
    tracker.ack(b)
    assert tracker.watermark == "b"


def test_out_of_order_acks_never_skip_an_unacked_event():
    tracker = AckTracker()
    a, b, c = (tracker.register(x) for x in "abc")
    tracker.ack(c)
    tracker.ack(b)
    assert tracker.watermark is None  # "a" is still in flight
    tracker.ack(a)
    assert tracker.watermark == "c"


def test_skipped_events_pass_but_not_beyond_an_in_flight_one():
    tracker = AckTracker()
    a = tracker.register("a")
    tracker.skip("b")
    assert tracker.watermark is None
    tracker.ack(a)
    assert tracker.watermark == "b"
