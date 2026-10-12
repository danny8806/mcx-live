from threading import Event, Thread

from core.timeframe_engine import Bar
from data.native_router import NativeCandleRouter


def _bar(end_ts):
    return Bar(
        instrument="GOLDM", timeframe="15m", start_ts=end_ts - 900,
        end_ts=end_ts, open=100, high=102, low=98, close=101,
    )


def test_concurrent_acceptance_cannot_publish_newer_candle_first():
    first_callback_entered = Event()
    release_first_callback = Event()
    delivered = []

    def distribute(bar):
        if bar.end_ts == 900:
            first_callback_entered.set()
            assert release_first_callback.wait(timeout=2)
        delivered.append(bar.end_ts)

    router = NativeCandleRouter(
        distribute, {"GOLDM": {"security_id": "test-security"}})
    result = []
    first = Thread(target=lambda: result.append(router.on_candle(_bar(900))))
    first.start()
    assert first_callback_entered.wait(timeout=2)

    # This call accepts N+1 while N is still in its callback. It must wait
    # for N's publication instead of overtaking it.
    second = Thread(target=lambda: result.append(router.on_candle(_bar(1800))))
    second.start()
    second.join(timeout=0.05)
    assert second.is_alive()

    release_first_callback.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert delivered == [900, 1800]
    assert result == [True, True]
    assert router.stats()["published"] == 2


def test_callback_exception_releases_next_accepted_candle():
    entered = Event()
    release = Event()
    delivered = []

    def distribute(bar):
        delivered.append(bar.end_ts)
        if bar.end_ts == 900:
            entered.set()
            assert release.wait(timeout=2)
            raise RuntimeError("injected distributor failure")

    router = NativeCandleRouter(
        distribute, {"GOLDM": {"security_id": "test-security"}})
    errors = []

    def submit_first():
        try:
            router.on_candle(_bar(900))
        except Exception as exc:  # the first caller retains callback failure
            errors.append(str(exc))

    first = Thread(target=submit_first)
    first.start()
    assert entered.wait(timeout=2)
    second_result = []
    second = Thread(target=lambda: second_result.append(
        router.on_candle(_bar(1800))))
    second.start()
    second.join(timeout=0.05)
    assert second.is_alive()

    release.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == ["injected distributor failure"]
    assert delivered == [900, 1800]
    assert second_result == [True]
