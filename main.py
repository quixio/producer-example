from quixstreams import Application
from random import randint, choice
from time import time_ns, sleep
import os

# create the app and specify your broker address.
app = Application(broker_address='KAFKA_BROKER_ADDRESS')

# define the topic that data will be output to.
topic = app.topic(os.environ["output"], key_serializer="str", value_serializer="json")

# create a word list to pick words from at random.
my_word_list = ["this", "that", "wow", "cool", "thing", "banana", "winning", "alpha", "thing", "read", "good", "bad"]
# create a list of user roles to use as the user creating the message.
roles = ["admin", "user", "mod", "guest"]
# generate some user id's.
users = [f"UID{i}" for i in range(20)]
# and a list of user roles.
user_roles = {user: choice(roles) for user in users}

try:
    # produce data to the Kafka topic using a QuixStreams producer.
    with app.get_producer() as producer:
        while True:
            words = ' '.join(my_word_list[:randint(0, len(my_word_list))])
            user = choice(users)
            msg = {"message": words, "role": user_roles[user], "timestamp": time_ns()}
            print(msg)
            msg_ser = topic.serialize(key=user, value=msg)
            producer.produce(key=msg_ser.key, value=msg_ser.value, topic=topic.name)
            sleep(randint(0, 20)*.1)
finally:
    print("shutting producer down")