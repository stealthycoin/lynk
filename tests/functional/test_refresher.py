import time

from lynk.refresh import LockRefresher


class TestLockRefresher(object):
    def noop(self):
        pass

    def test_can_create_refresher(self):
        refresher = LockRefresher(self.noop)
        assert isinstance(refresher, LockRefresher)

    def test_can_stop_refresher_quickly(self):
        refresher = LockRefresher(self.noop, refresh_period_seconds=1000)
        start = time.time()
        refresher.start()
        refresher.stop()
        end = time.time()
        diff = end - start
        tolerance = 0.001
        assert diff < tolerance

    def test_does_call_refresh_fn_periodically(self):

        refresh_count = 0

        def mark():
            nonlocal refresh_count
            refresh_count += 1

        refresher = LockRefresher(mark, refresh_period_seconds=0.1)
        refresher.start()
        time.sleep(0.35)
        refresher.stop()
        # In .35 seconds if we refresh every .1 second we should have refreshed
        # no fewer than 2 times and no more than 4 times.
        assert refresh_count < 4
        assert refresh_count > 2
