"""
   Sink helper

   Copyright 2016 22Acacia Systems, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

"""

import base64
import time
import logging
import sys
import os
import ConfigParser
from googleapiclient import discovery
import httplib2
from oauth2client.client import GoogleCredentials

PUBSUB_SCOPES = ["https://www.googleapis.com/auth/pubsub"]

def touch():
    """ used to touch a file for our health check """
    path = '/tmp/health'
    with open(path, 'a'):
        os.utime(path, None)

def get_logger(log_name, log_level):
    """Obtain a configured logger instance"""
    _logger = logging.getLogger(log_name)
    _logger.setLevel(log_level)

    format_str = '%(asctime)s\t%(levelname)s ' \
                 '-- %(processName)s %(filename)s:%(lineno)s -- %(message)s'
    formatter = logging.Formatter(format_str)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    stdout_handler.setLevel(log_level)

    _logger.addHandler(stdout_handler)
    _logger.propagate = False

    return _logger

class PubSubSubscription(object):
    """ object used to communicate with pubsub subscription """

    def __init__(self, config_file, _logger):
        # load config
        self.config = ConfigParser.ConfigParser()
        self.config.read(config_file)
        self.num_retries = self.config.getint('override', 'num_retries')
        self.batch_size = self.config.getint('override', 'batch_size')
        self.proj_name = self.config.get('override', 'proj_name')
        self.sub_name = self.config.get('override', 'sub_name')
        self.time_window = self.config.getint('override', 'time_window')
        self.log_level = self.config.get('override', 'log_level')
        self.logger = get_logger(__name__, self.log_level)

        self.ack_ids = []

        # check if batch_size is > 1000, if so decrease it to 1000
        if self.batch_size > 1000:
            self.batch_size = 1000

        # Google API setup
        credentials = GoogleCredentials.get_application_default()
        if credentials.create_scoped_required():
            credentials = credentials.create_scoped(PUBSUB_SCOPES)
        http = httplib2.Http()
        credentials.authorize(http=http)
        self.client = discovery.build('pubsub', 'v1', http=http)


    def fqrn(self, resource_type, project, resource):
        """Return a fully qualified resource name for Cloud Pub/Sub."""
        return "projects/{}/{}/{}".format(project, resource_type, resource)


    def get_full_subscription_name(self, project, subscription):
        """Return a fully qualified subscription name."""
        return self.fqrn('subscriptions', project, subscription)


    def wait_for_messages(self, all_msgs):
        """ Waits to get messages based on batch_size and time_window """
        start = time.time()
        while (len(all_msgs) < self.batch_size) and ((time.time() - start) < self.time_window):
            touch() # for periodic health check
            num_to_get = self.batch_size - len(all_msgs)
            self.logger.debug("# messages waiting for: %d" % num_to_get)
            subscription = self.get_full_subscription_name(
                self.proj_name,
                self.sub_name)
            body = {
                'returnImmediately': True,
                'maxMessages': num_to_get
            }
            sub_request = self.client.projects().subscriptions().pull(
                    subscription=subscription, body=body)
            resp = sub_request.execute()
            if 'receivedMessages' in resp:
                all_msgs.extend(resp.get('receivedMessages'))
                time.sleep(1)


    def pull_messages(self):
        """Pull messages from a given subscription."""
        all_msgs = []

        try:
            self.wait_for_messages(all_msgs)
        except Exception as err:
            self.logger.error(err)
            self.logger.error("Error pulling messages from subscription.")
        finally:
            rtnmessages = []
            self.logger.debug("# messages received: %d" % len(all_msgs))
            if all_msgs:
                self.ack_ids = []
                for received_message in all_msgs:
                    message = received_message.get('message')
                    if message:
                        rtnmessages.append(base64.b64decode(str(message.get('data'))))
                        self.ack_ids.append(received_message.get('ackId'))
            return rtnmessages


    def ack_messages(self):
        """ Acknowledges messages off of subscription """

        subscription = self.get_full_subscription_name(self.proj_name, self.sub_name)
        ack_body = {'ackIds': self.ack_ids }
        self.client.projects().subscriptions().acknowledge(subscription=subscription, body=ack_body).execute()
        self.logger.debug("# messages ack'd: %d" % len(self.ack_ids))
