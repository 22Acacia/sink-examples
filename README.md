# sink-examples

An example sink. Messages are pulled from a pubsub subscription that is specified in the config file. Messages are then processed by the 'do_something_to_message' function in main.py. Finally, the messages are acknowledged to remove them from the pubsub subscription.
