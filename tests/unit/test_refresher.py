import time
from threading import Thread

import mock

from lynk.refresh import LockRefresher
from lynk.refresh import LockRefresherFactory


class TestLockRefresher(object):
    def test_can_create_refresher(self):
        refresher = LockRefresher(lambda: None)
        assert isinstance(refresher, LockRefresher)

    def test_is_thread_subclass(self):
        refresher = LockRefresher(lambda: None)
        assert isinstance(refresher, Thread)

    def test_can_stop_thread(self):
        refresher = LockRefresher(lambda: None)
        refresher.start()
        assert refresher.isAlive()
        refresher.stop()
        # Schedule other thread so it can stop itself
        time.sleep(0.1)
        assert refresher.isAlive() is False

    def test_does_call_refresh_fn(self):
        refresh_fn = mock.Mock()
        refresher = LockRefresher(refresh_fn, 0)
        refresher.start()
        time.sleep(0.1)
        refresher.stop()
        assert refresh_fn.called


class TestLockRefresherFactory(object):
    def test_can_create(self):
        mock_lock = mock.Mock()
        factory = LockRefresherFactory()
        refresher = factory.create_lock_refresher(mock_lock, 5)
        assert isinstance(refresher, LockRefresher)
