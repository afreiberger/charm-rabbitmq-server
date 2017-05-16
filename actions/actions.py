#!/usr/bin/python
#
# Copyright 2016 Canonical Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import subprocess

sys.path.append('hooks/')

from charmhelpers.core.hookenv import (
    action_set,
    action_fail,
)

from rabbit_utils import (
    ConfigRenderer,
    CONFIG_FILES,
    RABBITMQ_CTL,
    pause_unit_helper,
    resume_unit_helper,
    list_vhosts,
)


def pause(args):
    """Pause the Ceilometer services.
    @raises Exception should the service fail to stop.
    """
    pause_unit_helper(ConfigRenderer(CONFIG_FILES))


def resume(args):
    """Resume the RabbitMQ services.
    @raises Exception should the service fail to start."""
    resume_unit_helper(ConfigRenderer(CONFIG_FILES))


def vhost_queue_info(vhost):
    try:
        output = subprocess.check_output([RABBITMQ_CTL,
                                          '-p', vhost, 'list_queues',
                                          'name', 'messages', 'consumers'])
    except subprocess.CalledProcessError as e:

        # if no queues, just raises an exception
        action_set({'output': e.output,
                    'return-code': e.returncoder})
        action_fail("Failed to query RabbitMQ vhost {} queues".format(vhost))
        return []

    queue_info = []
    if '...done' in output:
        queues = output.split('\n')[1:-2]
    else:
        queues = output.split('\n')[1:-1]

    for queue in queues:
        [qname, qmsgs, qconsumers] = queue.split()
        queue_info.append({
            'name': qname,
            'messages': int(qmsgs),
            'consumers': int(qconsumers)
        })

    return queue_info


def list_unconsumed_queues(args):
    """List queues which are unconsumed in RabbitMQ
    @raises Exception should the service fail to start."""
    count = 0
    for vhost in list_vhosts():
        iterator = 0
        for queue in vhost_queue_info(vhost):
            if queue['consumers'] == 0:
                vhostqueue = "unconsumed-queues.{}.{}".format(vhost, iterator)
                action_set({vhostqueue: "{} - {}".format(queue['name'],
                                                         queue['messages'])})
                iterator = iterator + 1
                count = count + 1

    action_set({'unconsumed-queue-count': count})


# A dictionary of all the defined actions to callables (which take
# parsed arguments).
ACTIONS = {"pause": pause, "resume": resume,
           "list-unconsumed-queues": list_unconsumed_queues}


def main(args):
    action_name = os.path.basename(args[0])
    try:
        action = ACTIONS[action_name]
    except KeyError:
        s = "Action {} undefined".format(action_name)
        action_fail(s)
        return s
    else:
        try:
            action(args)
        except Exception as e:
            action_fail("Action {} failed: {}".format(action_name, str(e)))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
