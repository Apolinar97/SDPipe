import os

import httpx

sd_pipe_api_url = os.getenv("SDPIPE_API")
sd_pipe_api_timeout = float(os.getenv("SDPIPE_API_TIMEOUT", "60"))


def call_generate_sql(user_message: str | None):
    with httpx.Client(timeout=sd_pipe_api_timeout) as client:
        response = client.post(f"{sd_pipe_api_url}/generate", json={"user_message": user_message})
        response.raise_for_status()
        return response.json()
