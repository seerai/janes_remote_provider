import logging
import requests
import time
import os
import traceback

from typing import List, Union, Tuple
import geopandas as gpd
from datetime import datetime as _datetime


from boson.http import serve
from boson.boson_core_pb2 import Property
from boson.conversion import cql2_to_query_params
from geodesic.cql import CQLFilter
from google.protobuf.timestamp_pb2 import Timestamp

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

ID_KEY = "id"
BASE_URL = "https://intara-api.janes.com/graph"
TOKEN_URL = "https://intara-api.janes.com/oauth/token"


class JanesIntara:
    def __init__(
        self,
        max_page_size: int = 1000,
        api_key: str = os.environ.get("API_KEY"),
        client_id: str = os.environ.get("CLIENT_ID"),
        client_secret: str = os.environ.get("CLIENT_SECRET"),
        defaults: dict = {},
    ) -> None:

        self.max_page_size = max_page_size
        self.api_key = api_key
        self.auth = {
            "clientId": client_id,
            "clientSecret": client_secret,
            "bearerToken": "",
        }

        self.default_component = "installations"
        self.api_default_params = defaults

    def _is_token_expired(self) -> bool:
        """
        Check if the token is expired
        """
        if "expires_at" in self.auth:
            return time.time() > self.auth["expires_at"]
        return True  # if 'expires_at' is not in self.auth, we assume the token is expired

    def ensure_token_valid(self) -> None:
        """
        Get bearer token from Janes API
        """
        if not self._is_token_expired():
            return

        headers = {"x-api-key": self.api_key}
        data = {"clientId": self.auth["clientId"], "clientSecret": self.auth["clientSecret"]}
        r = requests.post(TOKEN_URL, headers=headers, data=data)
        if r.status_code == 200:
            response = r.json()
            self.auth["bearerToken"] = f"Bearer {response['access_token']}"
            # Calculate the expiration time in clock time and store it
            expires_in = response["expires_in"]
            self.auth["expires_at"] = time.time() + expires_in
        else:
            logger.error(f"Error getting token: {r.status_code}")

        return

    def parse_input_params(
        self,
        bbox: List[float] = [],
        datetime: List[Timestamp] = [],
        intersects: object = None,
        feature_ids: List[str] = [],
        filter: Union[CQLFilter, dict] = None,
        fields: Union[List[str], dict] = None,
        component: str = None,
        sortby: dict = None,
        token: str = None,
        page: int = None,
        page_size: int = None,
        **kwargs,
    ) -> Tuple[str, dict]:
        """
        Translate geodesic input parameters to API parameters. This function accepts the boson search function
        parameters and returns a dictionary (api_params) with the parameters to be used in the API request.
        """
        api_params = {}
        api_url = f"{BASE_URL}/{component}"
        filters = []

        # Token based - does not need any other parameters
        if token:
            api_params["nextPageToken"] = token
            api_params["pageSize"] = page_size
            return api_url, api_params

        """
        DEFAULTS: Add default parameters to the request. TODO: Edit these in the __init__ method.
        """
        if self.api_default_params:
            api_params.update(self.api_default_params)

        """
        BBOX: Add the bbox to the request, if it was provided
        """
        if bbox:
            logger.info(f"Input bbox: {bbox}")
            filters.append(f"_within(({bbox[3]}, {bbox[0]}),({bbox[1]}, {bbox[2]}))")
        else:
            logger.info("No bbox provided")

        """
        DATETIME: datetimes are provided as a list of two timestamps. TODO: Convert to whatever the API expects
        """
        if datetime:
            logger.info(f"Received datetime: {datetime}")

            startdate = datetime[0].strftime("%Y-%m-%dT%H:%M:%SZ")
            enddate = datetime[1].strftime("%Y-%m-%dT%H:%M:%SZ")

            filters.append(f"lastModifiedDate:>={startdate}")
            filters.append(f"lastModifiedDate:<={enddate}")

        """
        INTERSECTS: Handle provided geometry. Unless the API accepts a geometry, this will be difficult to implement.
        In this example, we replace the bbox parameter with the bounding box of the geometry. This will provide
        some preliminary filtering, and then the results could be further filtered to fit the geometry after the
        features are returned.
        """
        if intersects:
            logger.info("Received geometry from intersects keyword. Adding geometry to filters.")
            # Example: take the bounds of the geometry and use as bbox
            g = intersects
            if intersects.geom_type != "Polygon":
                g = intersects.envelope

            coords = ", ".join([f"({c[0]}, {c[1]})" for c in g.exterior.coords])
            filters.append(f"_within({coords})")

        """
        IDS: Handle ids
        """
        if feature_ids:
            logger.info(f"Received ids of length: {len(feature_ids)}")
            if len(feature_ids) == 1:
                logger.info("Only one id received. Using id only endpoint")
                id_slug = feature_ids[0].split("/")[-1]
                api_url = f"{api_url}/{id_slug}"
                return api_url, {}
            else:
                api_params["ids"] = ",".join(feature_ids)

        """
        FILTER: Handle CQL2 filters. The cql2_to_query_params function will convert the CQL2 filter to a dictionary
            for cql filters with the "logical_and" and "eq" operators. The CQL filters are the way to pass api
            parameters to the search function.
        """
        if filter:
            logger.info("Received CQL filter")
            api_params.update(cql2_to_query_params(filter))

            if "filters" in api_params:
                filters.append(api_params["filters"])
                api_params.pop("filters")

        """
        FIELDS:  list of fields to include/exclude. Included fields should be prefixed by
        "+" and excluded fields by "-". Alernatively, a dict with a "include"/"exclude" lists
        may be provided
        """
        if fields:
            logger.info(f"Received fields: {fields}")
            if isinstance(fields, dict):
                include = fields.get("include", [])
            else:
                include = [field[1:] for field in fields if field[0] == "+"]

            api_params["fields"] = ",".join(include)

        """
        SORTBY: Handle sorting. Sortby is a dict containing “field” and “direction”.
        Direction may be one of “asc” or “desc”. Not supported by all datasets
        """
        if sortby:
            logger.info(f"Received sortby: {sortby}")
            field = sortby.get("field", None)
            if field:
                api_params["sort"] = f'{field}:{sortby.get("direction", "asc")}'

        """
        PAGINATION: Handle pagination (page and page_size)
        """
        api_params["pageNo"] = page
        api_params["pageSize"] = page_size

        api_params["filters"] = ",".join(filters)

        return api_url, api_params

    def convert_results_to_gdf(self, response: Union[dict, List[dict]]) -> gpd.GeoDataFrame:
        """
        Convert the response from the API to a GeoDataFrame.

        The template assumes point features and a single datetime, but this can be modified to handle other geometries
        and multiple datetimes. The remaining outputs from the API response can be added to the properties dictionary.
        """

        results = []
        if isinstance(response, dict):
            if "results" in response:
                results = response.pop("results", [])
            else:
                results = [response]

        logger.info(f"Received {len(results)} results. Converting to GeoDataFrame.")

        # Check for empty response
        if len(results) == 0:
            logger.info("No results found. Returning empty GeoDataFrame.")
            return gpd.GeoDataFrame(columns=["geometry", "id"])

        logger.info(f"First result: {results[0]}")

        lats = []
        lons = []
        datetimes = []

        for observation in results:
            # Extract the coordinates from the observation
            location = observation.get("locatedAt", {})
            if location:
                lat = location.get("lat", 0)
                lon = location.get("long", 0)
            elif "groupBasedAt" in observation:
                location = observation.get("groupBasedAt")
                if "locatedAt" in location:
                    location = location.get("locatedAt", {})
                lat = location.get("lat", 0)
                lon = location.get("long", 0)
            else:
                lat = lon = 0

            lats.append(lat)
            lons.append(lon)

            # get last modified date
            obs_datetime = observation.get("datetime", None)
            if obs_datetime:
                obs_datetime = _datetime.strptime(obs_datetime, "%Y-%m-%dT%H:%M:%S+00:00").strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )

            elif "lastModifiedDate" in observation:
                obs_datetime = observation.get("lastModifiedDate")
                obs_datetime = _datetime.strptime(obs_datetime, "%Y-%m-%dT%H:%M:%SZ").strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )

            else:
                obs_datetime = "NaT"

            datetimes.append(obs_datetime)

        gdf = gpd.GeoDataFrame(
            results,
            geometry=gpd.points_from_xy(lons, lats),
        )

        gdf.set_index(ID_KEY, inplace=True)

        logger.info(f"Converted {len(results)} results to GeoDataFrame of length {len(gdf)}")

        return gdf

    def request_features(self, **kwargs) -> gpd.GeoDataFrame:
        """
        Request data from the API and return a GeoDataFrame. This function is unlikely to need
        modification.
        """
        # Translate the input parameters to API parameters
        logger.info(f"Parsing search input parameters: {kwargs}")
        api_url, api_params = self.parse_input_params(**kwargs)

        # Make a GET request to the API
        logger.info(f"Making request to {api_url} with params: {api_params}")

        if "filters" in api_params and not api_params["filters"]:
            api_params.pop("filters")

        # Check and add authorization
        self.ensure_token_valid()

        headers = {"x-api-key": self.api_key, "Authorization": self.auth["bearerToken"]}
        logger.info(f"Requesting data from {api_url}")

        response = requests.get(api_url, headers=headers, params=api_params)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse and use the response data (JSON in this case)
            res = response.json()

            # Check if the response is empty
            if not res:
                logger.info("No results found. Returning empty GeoDataFrame.")
                return gpd.GeoDataFrame(columns=["geometry", "id"]), None

            count_only = kwargs.get("count_only", False)
            if count_only:
                count = None
                if "search" in res:
                    count = res["search"].get("totalResults")
                    return count, None
                return gpd.GeoDataFrame(columns=["geometry", "id"]), None

            gdf = self.convert_results_to_gdf(res)
            logger.info(f"Received {len(gdf)} features")
        else:
            logger.error(f"Error: {response.status_code}")
            msg = ""
            try:
                msg = response.text
                logger.error(response.text)
            except Exception:
                pass
            raise ValueError(f"Janes Status Code: {response.status_code}, Error: {msg}")

        # For searches with > 10000 results, tokens must be used
        search = res.get("search", {})
        total_results = search.get("totalResults", 0)
        token = None
        if total_results >= 10000:
            token = search.get("nextPageToken")

        return gdf, token

    def search(
        self, pagination={}, provider_properties={}, count_only: bool = False, **kwargs
    ) -> gpd.GeoDataFrame:
        """Implements the Boson Search endpoint."""
        logger.info("Making request to API.")
        logger.info(f"Search received kwargs: {kwargs}")

        # Disable use of counts for this provider - this will force ArcGIS to request tiles for better rendering
        enable_counts = provider_properties.get("enable_counts", False)
        if not enable_counts and count_only:
            return {}

        """
        PAGINATION and LIMIT: Janes Intara can handle pagination by either next/prev token or page/page_size.
        """
        # Returning an empty out makes ArcGIS use tiled requests which has better mapping performance unless
        # random access pagination is used. Random access pagination is not supported
        page, page_size, token = self.parse_pagination(
            pagination, kwargs.get("limit", self.max_page_size)
        )

        """
        PROVIDER_PROPERTIES: These are the properties set in the boson_config.properties. 
        These are used to determine which component of the Janes API to search (installations (default), units, etc)
        """
        component = provider_properties.get("component", self.default_component)
        if component is None:
            logger.error("No component provided in provider_properties")
            raise ValueError("No component provided in provider_properties")

        # Make the request to Janes
        gdf, token = self.request_features(
            token=token,
            page=page,
            page_size=page_size,
            component=component,
            count_only=count_only,
            use_counts=enable_counts,
            **kwargs,
        )

        logger.info("returning search")
        logger.info(f"Returning gdf of type: {type(gdf)} with length {len(gdf)}")

        if not token:
            pagination_dict = {"page": page + 1, "page_size": page_size}
        else:
            pagination_dict = {"token": token}

        logger.info(f"next pagination: {pagination_dict}")
        return gdf, pagination_dict

    def parse_pagination(self, pagination: dict, limit: int) -> Tuple[int, int, str]:
        """parses the pagination from the request. Both page/page_size and tokens are supported"""
        page = 1
        page_size = limit
        token = None

        if limit == 0:
            page_size = 10

        if page_size > self.max_page_size:
            raise ValueError(
                f"page_size of {page_size} is greater than the max_page_size of {self.max_page_size}"
            )

        if pagination and "page" in pagination and "page_size" in pagination:
            logger.info(f"Received pagination: {pagination}")
            page = pagination.get("page", None)
            if page == 0:
                logger.info("Received page 0. Setting page to 1")
                page = 1

            page_size = pagination.get("page_size", self.max_page_size)
        elif pagination and "token" in pagination:
            token = pagination.get("token")
            logger.info(f"Received token: {token}")

        return page, page_size, token

    def queryables(self, **kwargs) -> dict:
        """
        Update this method to return a dictionary of queryable parameters that the API accepts.
        The keys should be the parameter names. The values should be a Property object that follows
        the conventions of JSON Schema.
        """
        return {
            "ids": Property(
                title="ids",
                type="string",
            ),
            "q": Property(
                title="search_query",
                type="string",
            ),
            "facets": Property(
                title="facets",
                type="string",
            ),
            "dateFacets": Property(
                title="dateFacets",
                type="string",
            ),
            "facetSize": Property(
                title="facetSize",
                type="integer",
            ),
        }


janes_installations = JanesIntara()
app = serve(search_func=janes_installations.search, queryables_func=janes_installations.queryables)
