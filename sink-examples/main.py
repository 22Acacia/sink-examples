"""
   Sink launcher

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

import ConfigParser
import sinks

# name of the config file we're using
CONFIG_FILE = "config"

def do_something_to_message(message):
    """ This function contains any operations we want to perform
        to each message as we receive them """
    # In this example, we will just print the message
    # In a more useful case, anything that we want to do to the messages,
    # for instance writing them to a file, could be put here
    print message


def main():
    """ Loads configuration and starts sink """

    # Load Configuration
    config = ConfigParser.ConfigParser()
    config.read(CONFIG_FILE)
    log_level = config.get('override', 'log_level')
    logger = sinks.get_logger(__name__, log_level)

    # connect to pubsub subscription
    # We will use this to get messages out of pubsub
    subscrip = sinks.PubSubSubscription(CONFIG_FILE, logger)

    # This is the main body of the program
    # We should execute this indefinitely
    while True:
        try:
            # Try to get messages from pubsub
            messages = subscrip.pull_messages()

            # If there are any messages
            if messages:
                # For each message we've collected...
                for message in messages:
                    # ... we will do something to it
                    # See the function 'do_something_to_message' above
                    do_something_to_message(message)

                # once we are done with the messages, we must acknowledge
                # them. Ff we don't, they will reappear on the subscription
                # a time out.
                subscrip.ack_messages()

        except Exception as err:
            logger.error(err)

if __name__ == '__main__':
    main()
