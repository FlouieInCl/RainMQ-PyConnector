import requests
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Dict
from json import dumps


class RainMQProducer:
    def __init__(self, *, mq_server: str, topic_name: str):
        self.mq_server = mq_server
        self.topic_name = topic_name

    def _send_with_thread_executor(
        self,
        method: str,
        additional_headers: Dict[str: str],
        query_string: Dict[str: str],
        json: dict,
    ):
        return requests.request(
            method,
            self.mq_server,
            headers=additional_headers,
            params=query_string,
            data=dumps(json),
        )

    def send(
        self,
        method: str,
        additional_headers: Dict[str: str],
        query_string,
        json: dict,
    ) -> Future:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future: Future = executor.submit(
                self._send_with_thread_executor,
                method,
                additional_headers,
                query_string,
                json
            )

        return future
