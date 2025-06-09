import logging
from requests_sse import EventSource
import json
from pprint import pformat
from quixstreams import Application


def main() -> None:
    logging.info("Start")
    url: str = "http://github-firehose.libraries.io/events"
    method: str = "GET"
    connect_to_event_source(url, method)


def connect_to_event_source(url: str, method: str):
    try:
        with EventSource(url, method, timeout=30) as eventSource:
            for event in eventSource:
                value: dict = json.loads(event.data)
                identification: str = value.get("id")
                creation_date_time: str = value.get("created_at")
                public: bool = value.get("public")
                repo: dict = value.get("repo")
                publish_event_to_kafka(identification, creation_date_time, public, repo)

    except KeyboardInterrupt:
        logging.info("Terminating due to keyboard interrupt!")


def publish_event_to_kafka(key: str, created_date_time: str, public: bool = None, repo: dict = None):
    public = public if public is not None else False
    repo = repo if repo is not None else "Empty"
    repo_bytes = json.dumps(repo).encode("utf-8")

    with Application(broker_address="10.1.1.50:9092", loglevel="DEBUG").get_producer() as producer:
        producer.produce(topic="GitHub-firehose", key=key.encode("utf-8"), value=repo_bytes, headers={"Public": str(public)})


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()
