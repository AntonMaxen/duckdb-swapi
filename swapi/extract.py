import datetime
import json

import requests
import tenacity
from requests import exceptions
from tenacity import retry_if_exception_type

from swapi.config import Config


# Configuration for tenacity retry.
RETRY_ARGS = dict(
    wait=tenacity.wait.wait_exponential(min=1, max=60, multiplier=2),
    stop=tenacity.stop_after_attempt(5),
    retry=retry_if_exception_type(exception_types=(exceptions.ConnectionError, exceptions.HTTPError)),
)


def fetch_and_raise_for_status(*args, **kwargs) -> requests.Response:
    """wrapped function used in combination with tenacity retry."""
    response = requests.get(*args, **kwargs)
    response.raise_for_status()

    return response


def get_all_results(endpoint: str) -> list[dict]:
    """Get all results for a specific swapi endpoint."""
    resources = []

    while True:
        # Unstable api needs some retrying when requesting.
        response = tenacity.Retrying(**RETRY_ARGS)(
            fetch_and_raise_for_status, endpoint, headers={"Accept": "application/json"}
        )

        # Save everything to joined result list.
        data = response.json()
        resources.extend(data["results"])

        # Results are split into chunks
        # Next chunk of results pointed in next value.
        if data["next"] is not None:
            endpoint = data["next"]
        else:
            # if no next we are done.
            break

    return resources


def extract_resources():
    """Extract resources from swapi endpoints."""
    Config.working_directory.mkdir(parents=True, exist_ok=True)
    resource_data = {}

    for i, resource in enumerate(Config.resources):
        print(f"[{i+1}/{len(Config.resources)+1}]: Extracting resource {resource}")

        # Fetch all results for current endpoint.
        endpoint = f"{Config.base_url}/{resource}"
        results = get_all_results(endpoint)

        # Save results to unique file.
        current_timestring = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filepath = Config.working_directory / f"extract__{resource}__{current_timestring}.json"
        with output_filepath.open("w", encoding="utf8") as file:
            json.dump(results, file, indent=4)

        resource_data[resource] = str(output_filepath)

    return resource_data
