# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
import os

from rptest.services.cluster import cluster
from ducktape.mark import matrix, ok_to_fail

from rptest.services.librdkafka_test_case import LibrdkafkaTestcase
from rptest.tests.redpanda_test import RedpandaTest

from rptest.utils.mode_checks import skip_debug_mode


def tests_to_run():
    ignored_tests = {
        14,
        51,
        67,
        90,
        99,
        103,
        52,
        61,
        66,
        77,
        81,
        82,
        92,
        109,
        113,
        115,
        119,
        30,
        84,
        105,
    }
    return [t for t in range(120) if t not in ignored_tests]


class LibrdkafkaTest(RedpandaTest):
    """
    Execute the librdkafka test suite against redpanda.
    """
    TESTS_DIR = "/opt/librdkafka/tests"
    CONF_FILE = os.path.join(TESTS_DIR, "test.conf")

    def __init__(self, context):
        super(LibrdkafkaTest, self).__init__(context,
                                             num_brokers=3,
                                             extra_rp_conf={
                                                 "auto_create_topics_enabled":
                                                 True,
                                                 "default_topic_partitions": 4
                                             })

    @ok_to_fail  # https://github.com/redpanda-data/redpanda/issues/7148
    @cluster(num_nodes=4)
    @matrix(test_num=tests_to_run())
    @skip_debug_mode
    def test_librdkafka(self, test_num):
        tc = LibrdkafkaTestcase(self.test_context, self.redpanda, test_num)
        tc.start()
        tc.wait()

        assert tc.error is None, f"Failure in librdkafka test case {test_num:04}"
