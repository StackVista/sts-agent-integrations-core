# stdlib
import os

from tests.checks.common import AgentCheckTest
from nose.plugins.attrib import attr
import datetime
import pytz

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), 'ci')


@attr(requires='xl-deploy')
class TestXLDeployContinueAfterRestart(AgentCheckTest):
    """
    XL-Deploy should continue after restart
    """
    CHECK_NAME = 'xl-deploy'

    def test_checks(self):
        self.maxDiff = None

        config = {
            'init_config': {},
            'instances': [{
                'url': 'http://localhost:4516/deployit/repository',
                'user': "admin",
                'pass': "pass"
            }]
        }

        self.expected_timestamp = datetime.datetime(1970, 1, 1)

        def _mock_current_timestamp():
            """
            will be invoked after commit has succeeded
            :return: datetime timestamp
            """
            ts = datetime.datetime.now(tz=pytz.utc).isoformat()
            self.expected_timestamp = ts
            return ts

        def _mock_get_methods(_, ts):
            """
            mock getter functions that actually do the requests to check the received timestamp, should be set by
            _mock_current_timestamp in previous check run.
            """
            self.assertEqual(ts, self.expected_timestamp, msg="%s != expected %s" % (ts, self.expected_timestamp))
            pass

        # Initial test run ignores the loaded timestamp
        self.run_check(config, mocks={
            'current_timestamp': _mock_current_timestamp,
            'get_topology': lambda x, y: None,
            'get_deployments': lambda x, y: None
        })

        # execute check three times
        for i in xrange(3):
            self.run_check(config, mocks={
                'current_timestamp': _mock_current_timestamp,
                'get_topology': _mock_get_methods,
                'get_deployments': _mock_get_methods
            })
            self.assertEqual(self.check._persistable_store['recent_timestamp'], self.expected_timestamp)