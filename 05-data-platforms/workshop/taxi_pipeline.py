"""dlt pipeline to ingest NYC Yellow Taxi trips from the Zoomcamp demo API.

The API returns paginated JSON (1,000 records per page) and stops when
an empty page is returned.
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


@dlt.resource(name="rides")
def ny_taxi_rides():
    """Yield pages of NYC taxi rides from the demo API.

    The REST client handles page-by-page iteration using a page-number paginator.
    """

    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None,
        ),
    )

    # The paginator will keep requesting pages until the API returns an empty page
    # (i.e. an empty list), at which point iteration stops.
    for page in client.paginate("data_engineering_zoomcamp_api"):
        yield page


@dlt.source(name="taxi_source")
def taxi_source():
    """dlt source wrapping the NYC taxi rides resource."""

    return ny_taxi_rides()


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="taxi_data",
        progress="log",
    )

    load_info = pipeline.run(taxi_source())
    print(load_info)
