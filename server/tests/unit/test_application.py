from recceiver.application import RecService
from recceiver.recast import CollectionSession


class TestRecServiceConfig:
    def test_commit_size_limit_defaults_to_class_default(self):
        svc = RecService({})
        assert svc.commitSizeLimit == CollectionSession.trlimit

    def test_commit_size_limit_reads_from_config(self):
        svc = RecService({"commitSizeLimit": "100"})
        assert svc.commitSizeLimit == 100

    def test_commit_size_limit_zero_disables_splitting(self):
        svc = RecService({"commitSizeLimit": "0"})
        assert svc.commitSizeLimit == 0
