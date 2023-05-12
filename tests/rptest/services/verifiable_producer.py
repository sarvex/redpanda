# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Modifications Copyright 2021 Redpanda Data, Inc.
# - Reformatted code
# - Replaced dependency on Kafka with Redpanda
# - Imported a is_int_* helper functions from ducktape test suite

import json
import os
import time
import signal
from collections import namedtuple

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.background_thread import BackgroundThreadService

TopicPartition = namedtuple('TopicPartition', ['topic', 'partition'])


def is_int(msg):
    """Method used to check whether the given message is an integer

    return int or raises an exception if message is not an integer
    """
    try:
        return int(msg)
    except ValueError:
        raise Exception(
            f"Unexpected message format (expected an integer). Message: {msg}"
        )


def is_int_with_prefix(msg):
    """
    Method used check whether the given message is of format 'integer_prefix'.'integer_value'

    :param msg: message to validate
    :return: msg or raises an exception is a message is of wrong format
    """
    try:
        parts = msg.split(".")
        if len(parts) != 2:
            raise Exception(
                "Unexpected message format. Message should be of format: integer "
                "prefix dot integer value. Message: %s" % (msg))
        int(parts[0])
        int(parts[1])
        return msg
    except ValueError:
        raise Exception(
            "Unexpected message format. Message should be of format: integer "
            "prefix dot integer value, but one of the two parts (before or after dot) "
            "are not integers. Message: %s" % (msg))


class VerifiableProducer(BackgroundThreadService):
    """
    This service wraps org.apache.kafka.tools.VerifiableProducer for use in
    system testing.
    """

    PERSISTENT_ROOT = "/mnt/verifiable_producer"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT,
                                  "verifiable_producer.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT,
                                  "verifiable_producer.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "verifiable_producer.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT,
                               "verifiable_producer.properties")

    logs = {
        "verifiable_producer_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": False
        },
        "verifiable_producer_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": False
        },
        "verifiable_producer_log": {
            "path": LOG_FILE,
            "collect_default": True
        }
    }

    def __init__(self,
                 context,
                 num_nodes,
                 redpanda,
                 topic,
                 max_messages=-1,
                 throughput=100000,
                 message_validator=is_int,
                 compression_types=None,
                 acks=None,
                 stop_timeout_sec=150,
                 request_timeout_sec=30,
                 log_level="INFO",
                 enable_idempotence=False,
                 create_time=-1,
                 repeating_keys=None,
                 retries=None):
        """
        Args:
            :param max_messages                number of messages to be produced per producer
            :param message_validator           checks for an expected format of messages produced. There are
                                               currently two:
                                               * is_int is an integer format; this is default and expected to be used if
                                                 num_nodes = 1
                                               * is_int_with_prefix recommended if num_nodes > 1, because otherwise each producer
                                                 will produce exactly same messages, and validation may miss missing messages.
            :param compression_types           If None, all producers will not use compression; or a list of compression types,
                                               one per producer (could be "none").
        """
        super(VerifiableProducer, self).__init__(context, num_nodes)
        self.log_level = log_level

        self.redpanda = redpanda
        self.topic = topic
        self.max_messages = max_messages
        self.throughput = throughput
        self.message_validator = message_validator
        self.compression_types = compression_types
        if self.compression_types is not None:
            assert len(self.compression_types
                       ) == num_nodes, "Specify one compression type per node"

        self.acked_values = []
        self.acked_values_by_partition = {}
        self._last_acked_offsets = {}
        self.not_acked_values = []
        self.produced_count = {}
        self.clean_shutdown_nodes = set()
        self.acks = acks
        self.stop_timeout_sec = stop_timeout_sec
        self.request_timeout_sec = request_timeout_sec
        self.enable_idempotence = enable_idempotence
        self.create_time = create_time
        self.repeating_keys = repeating_keys
        self.retries = retries

    def prop_file(self, node):
        idx = self.idx(node)
        prop_file = self.render('producer.properties',
                                request_timeout_ms=(self.request_timeout_sec *
                                                    1000))
        if self.compression_types is not None:
            compression_index = idx - 1
            self.logger.info(
                "VerifiableProducer (index = %d) will use compression type = %s",
                idx, self.compression_types[compression_index])
            prop_file += "\ncompression.type=%s\n" % self.compression_types[
                compression_index]
        return prop_file

    def _worker(self, idx, node):
        node.account.ssh(
            f"mkdir -p {VerifiableProducer.PERSISTENT_ROOT}", allow_fail=False
        )

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties',
                                 log_file=VerifiableProducer.LOG_FILE)
        node.account.create_file(VerifiableProducer.LOG4J_CONFIG, log_config)

        # Create and upload config file
        producer_prop_file = self.prop_file(node)

        if self.acks is not None:
            self.logger.info(
                "VerifiableProducer (index = %d) will use acks = %s", idx,
                self.acks)
            producer_prop_file += "\nacks=%s\n" % self.acks

        if self.enable_idempotence:
            self.logger.info("Setting up an idempotent producer")
            producer_prop_file += "\nmax.in.flight.requests.per.connection=5\n"
            producer_prop_file += "\nretries=1000000\n"
            producer_prop_file += "\nenable.idempotence=true\n"
        elif self.retries is not None:
            self.logger.info(
                "VerifiableProducer (index = %d) will use retries = %s", idx,
                self.retries)
            producer_prop_file += "\nretries=%s\n" % self.retries
            producer_prop_file += "\ndelivery.timeout.ms=%s\n" % (
                self.request_timeout_sec * 1000 * self.retries)

        self.logger.info("verifiable_producer.properties:")
        self.logger.info(producer_prop_file)
        node.account.create_file(VerifiableProducer.CONFIG_FILE,
                                 producer_prop_file)

        cmd = self.start_cmd(idx)
        self.logger.debug("VerifiableProducer %d command: %s" % (idx, cmd))

        self.produced_count[idx] = 0
        last_produced_time = time.time()
        prev_msg = None

        for line in node.account.ssh_capture(cmd):
            line = line.strip()

            data = self.try_parse_json(line)
            if data is not None:

                with self.lock:
                    if data["name"] == "producer_send_error":
                        data["node"] = idx
                        value = self.message_validator(data["value"])
                        key = data["key"]
                        self.not_acked_values.append((key, value))
                        self.produced_count[idx] += 1

                    elif data["name"] == "producer_send_success":
                        partition = TopicPartition(data["topic"],
                                                   data["partition"])
                        value = self.message_validator(data["value"])
                        key = data["key"]
                        self.acked_values.append((key, value))

                        if partition not in self.acked_values_by_partition:
                            self.acked_values_by_partition[partition] = []
                        self.acked_values_by_partition[partition].append(value)

                        self.produced_count[idx] += 1

                        # Completions are not guaranteed to be called in-order wrt offsets,
                        # even if there is only one producer, so we must handle situation
                        # where we see an offset lower than what we already recorded as highest.
                        self._last_acked_offsets[partition] = max(
                            data["offset"],
                            self._last_acked_offsets.get(partition, 0))

                        # Log information if there is a large gap between successively acknowledged messages
                        t = time.time()
                        time_delta_sec = t - last_produced_time
                        if time_delta_sec > 2 and prev_msg is not None:
                            self.logger.debug(
                                f"Time delta between successively acked messages is large: delta_t_sec: {str(time_delta_sec)}, prev_message: {str(prev_msg)}, current_message: {str(data)}"
                            )

                        last_produced_time = t
                        prev_msg = data

                    elif data["name"] == "shutdown_complete":
                        if node in self.clean_shutdown_nodes:
                            raise Exception(
                                "Unexpected shutdown event from producer, already shutdown. Producer index: %d"
                                % idx)
                        self.clean_shutdown_nodes.add(node)

    def start_cmd(self, idx):
        cmd = "java -cp /opt/redpanda-tests/java/e2e-verifiers/target/e2e-verifiers-1.0.jar"
        cmd += f" -Dlog4j.configuration=file:{VerifiableProducer.LOG4J_CONFIG}"
        cmd += " org.apache.kafka.tools.VerifiableProducer "
        cmd += f" --topic {self.topic} --broker-list {self.redpanda.brokers()}"
        if self.max_messages > 0:
            cmd += f" --max-messages {str(self.max_messages)}"
        if self.throughput > 0:
            cmd += f" --throughput {str(self.throughput)}"
        if self.message_validator == is_int_with_prefix:
            cmd += f" --value-prefix {str(idx)}"
        if self.acks is not None:
            cmd += f" --acks {str(self.acks)} "
        if self.create_time > -1:
            cmd += f" --message-create-time {str(self.create_time)} "
        if self.repeating_keys is not None:
            cmd += f" --repeating-keys {str(self.repeating_keys)} "

        cmd += f" --producer.config {VerifiableProducer.CONFIG_FILE}"

        cmd += f" 2>> {VerifiableProducer.STDOUT_CAPTURE} | tee -a {VerifiableProducer.STDOUT_CAPTURE} &"
        return cmd

    def kill_node(self, node, clean_shutdown=True, allow_fail=False):
        sig = signal.SIGKILL if not clean_shutdown else signal.SIGTERM
        for pid in self.pids(node):
            node.account.signal(pid, sig, allow_fail)

    def pids(self, node):
        try:
            cmd = "jps | grep -i VerifiableProducer | awk '{print $1}'"
            return list(
                node.account.ssh_capture(cmd, allow_fail=True, callback=int)
            )
        except (RemoteCommandError, ValueError):
            return []

    def alive(self, node):
        return len(self.pids(node)) > 0

    @property
    def last_acked_offsets(self):
        with self.lock:
            return self._last_acked_offsets

    @property
    def acked(self):
        with self.lock:
            return self.acked_values

    @property
    def acked_by_partition(self):
        with self.lock:
            return self.acked_values_by_partition

    @property
    def not_acked(self):
        with self.lock:
            return self.not_acked_values

    @property
    def num_acked(self):
        with self.lock:
            return len(self.acked_values)

    @property
    def num_not_acked(self):
        with self.lock:
            return len(self.not_acked_values)

    def each_produced_at_least(self, count):
        with self.lock:
            return not any(
                self.produced_count.get(idx) is None
                or self.produced_count[idx] < count
                for idx in range(1, self.num_nodes + 1)
            )

    def stop_node(self, node):
        # There is a race condition on shutdown if using `max_messages` since the
        # VerifiableProducer will shutdown automatically when all messages have been
        # written. In this case, the process will be gone and the signal will fail.
        allow_fail = self.max_messages > 0
        self.kill_node(node, clean_shutdown=True, allow_fail=allow_fail)
        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert (
            stopped
        ), f"Node {str(node.account)}: did not stop within the specified timeout of {str(self.stop_timeout_sec)} seconds"

    def clean_node(self, node):
        self.kill_node(node, clean_shutdown=False, allow_fail=False)
        node.account.ssh(f"rm -rf {self.PERSISTENT_ROOT}", allow_fail=False)

    def try_parse_json(self, string):
        """Try to parse a string as json. Return None if not parseable."""
        try:
            return json.loads(string)
        except ValueError:
            self.logger.debug(f"Could not parse as json: {str(string)}")
            return None
