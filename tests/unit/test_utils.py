from lynk.utils import TimeUtils


class TestTimeUtils(object):
    def test_can_create_utils(self):
        utils = TimeUtils()
        assert isinstance(utils, TimeUtils)
