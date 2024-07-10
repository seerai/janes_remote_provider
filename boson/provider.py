import logging
import requests
import time
import os

from typing import List, Union
import geopandas as gpd
from datetime import datetime as _datetime


from boson.http import serve
from boson.boson_core_pb2 import Property
from boson.conversion import cql2_to_query_params
from geodesic.cql import CQLFilter
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.struct_pb2 import Struct, ListValue

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class JanesInstallationsRemoteProvider:
    def __init__(self) -> None:
        self.base_url = "https://intara-api.janes.com/graph/"
        self.api_url = "https://intara-api.janes.com/graph/military-groups"
        self.oauth_url = "https://intara-api.janes.com/oauth/token"
        self.default_page_size = 500
        self.api_key = os.environ.get("API_KEY")
        self.auth = {
            "clientId": os.environ.get("CLIENT_ID"),
            "clientSecret": os.environ.get("CLIENT_SECRET"),
            "bearerToken": "",
        }
        self.api_default_params = {}
        self.filters = ""

    def is_token_expired(self) -> bool:
        """
        Check if the token is expired
        """
        if "expires_at" in self.auth:
            return time.time() > self.auth["expires_at"]
        return True  # if 'expires_at' is not in self.auth, we assume the token is expired

    def get_token(self) -> None:
        """
        Get bearer token from Janes API
        """
        headers = {"x-api-key": self.api_key}
        data = {"clientId": self.auth["clientId"], "clientSecret": self.auth["clientSecret"]}
        r = requests.post(self.oauth_url, headers=headers, data=data)
        if r.status_code == 200:
            response = r.json()
            self.auth["bearerToken"] = f"Bearer {response['access_token']}"
            # Calculate the expiration time in clock time and store it
            expires_in = response["expires_in"]
            self.auth["expires_at"] = time.time() + expires_in
        else:
            logger.error(f"Error getting token: {r.status_code}")

        return

    def update_filters(self, new_filter: str) -> str:
        """
        Update the filters string with a new filter. This function is used to add filters to the request.
        """
        if self.filters:
            self.filters += f",{new_filter}"
        else:
            self.filters = new_filter
        return

    def parse_input_params(
        self,
        bbox: List[float] = [],
        datetime: List[Timestamp] = [],
        intersects: object = None,
        collections: List[str] = [],
        feature_ids: List[str] = [],
        filter: Union[CQLFilter, dict] = None,
        fields: Union[List[str], dict] = None,
        sortby: dict = None,
        method: str = "POST",
        page: int = None,
        page_size: int = None,
        **kwargs,
    ) -> dict:
        """
        Translate geodesic input parameters to API parameters. This function accepts the boson search function
        parameters and returns a dictionary (api_params) with the parameters to be used in the API request.
        """
        api_params = {}

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
            self.update_filters(f"_within(({bbox[3]}, {bbox[0]}),({bbox[1]}, {bbox[2]}))")
        else:
            logger.info("No bbox provided")

        """
        DATETIME: datetimes are provided as a list of two timestamps. TODO: Convert to whatever the API expects
        """
        if datetime:
            logger.info(f"Received datetime: {datetime}")

            startdate = datetime[0].strftime("%Y-%m-%dT%H:%M:%SZ")
            enddate = datetime[1].strftime("%Y-%m-%dT%H:%M:%SZ")

            self.update_filters(f"lastModifiedDate:>={startdate},lastModifiedDate:<={enddate}")

        """
        INTERSECTS: Handle provided geometry. Unless the API accepts a geometry, this will be difficult to implement.
        In this example, we replace the bbox parameter with the bounding box of the geometry. This will provide
        some preliminary filtering, and then the results could be further filtered to fit the geometry after the 
        features are returned.
        """
        if intersects:
            logger.info(f"Received geometry from intersects keyword. Adding geometry to filters.")
            # Example: take the bounds of the geometry and use as bbox
            bbox = intersects.bounds
            geometry_coords = intersects.coordinates
            self.update_filters(f"_within({','.join(geometry_coords)})")

        """ 
        COLLECTIONS: Handle collections, if applicable. Not implemented in this example.
        """
        if collections:
            logger.info(f"Received collections: {collections}. Updating url to include collections.")
            self.api_url = self.base_url + collections[0]

        """
        IDS: Handle ids
        """
        if feature_ids:
            logger.info(f"Received ids of length: {len(feature_ids)}")
            if len(feature_ids) == 1:
                logger.info("Only one id received. Using id only endpoint")
                id_slug = feature_ids[0].split("/")[-1]
                self.api_url = self.api_url + f"/{id_slug}"
                self.filters = ""
                return {}
            else:
                api_params["ids"] = ",".join(feature_ids)

        """
        FILTER: Handle CQL2 filters. The cql2_to_query_params function will convert the CQL2 filter to a dictionary
            for cql filters with the "logical_and" and "eq" operators. The CQL filters are the way to pass api parameters to the
            search function.
        """
        if filter:
            logger.info(f"Received CQL filter")
            api_params.update(cql2_to_query_params(filter))

            if "filters" in api_params:
                self.update_filters(api_params["filters"])
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
                include = [field for field in fields if field[0] == "+"]

            api_params["fields"] = ",".join(include)

        """
        SORTBY: Handle sorting. Sortby is a dict containing “field” and “direction”. 
        Direction may be one of “asc” or “desc”. Not supported by all datasets
        """
        if sortby:
            logger.info(f"Received sortby: {sortby}")
            api_params["sort"] = sortby.get("direction", "asc")

        """
        PAGINATION: Handle pagination (page and page_size)
        """
        if "pageNo" in self.queryables():
            api_params["pageNo"] = page
        if "pageSize" in self.queryables():
            api_params["pageSize"] = page_size

        return api_params

    def convert_results_to_gdf(self, response: Union[dict, List[dict]]) -> gpd.GeoDataFrame:
        """
        Convert the response from the API to a GeoDataFrame.

        The template assumes point features and a single datetime, but this can be modified to handle other geometries
        and multiple datetimes. The remaining outputs from the API response can be added to the properties dictionary.
        """
        features = []

        if isinstance(response, dict):
            if "results" in response:
                response = response.get("results", [])
            else:
                response = [response]

        logger.info(f"Received {len(response)} results. Converting to GeoDataFrame.")

        # Check for empty response
        if len(response) == 0:
            logger.info("No results found. Returning empty GeoDataFrame.")
            return gpd.GeoDataFrame(columns=["geometry", "id"])

        logger.info(f"First result: {response[0]}")

        ID_KEY = "id"

        lats = []
        lons = []
        datetimes = []

        for observation in response:

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
                obs_datetime = _datetime.strptime(obs_datetime, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%dT%H:%M:%SZ")

            else:
                obs_datetime = "NaT"

            datetimes.append(obs_datetime)

        gdf = gpd.GeoDataFrame(
            response,
            geometry=gpd.points_from_xy(lons, lats),
        )

        gdf.set_index(ID_KEY, inplace=True)

        logger.info(f"Converted {len(response)} results to GeoDataFrame of length {len(gdf)}")

        return gdf

    def request_features(self, **kwargs) -> gpd.GeoDataFrame:
        """
        Request data from the API and return a GeoDataFrame. This function is unlikely to need
        modification.
        """
        # Translate the input parameters to API parameters
        logger.info(f"Parsing search input parameters: {kwargs}")
        api_params = self.parse_input_params(**kwargs)

        # Make a GET request to the API
        logger.info(f"Making request to {self.api_url} with params: {api_params}")
        logger.info(f"Filters: {self.filters}")
        if self.filters:
            url = f"{self.api_url}?filters={self.filters}"
        else:
            url = self.api_url

        # Check and add authorization
        if self.is_token_expired():
            self.get_token()

        headers = {"x-api-key": self.api_key, "Authorization": self.auth["bearerToken"]}

        logger.info(f"Requesting data from {url} with headers: {headers}")

        response = requests.get(url, headers=headers, params=api_params)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse and use the response data (JSON in this case)
            res = response.json()

            # Check if the response is empty
            if not res:
                logger.info("No results found. Returning empty GeoDataFrame.")
                return gpd.GeoDataFrame(columns=["geometry", "id"])

            gdf = self.convert_results_to_gdf(res)
            logger.info(f"Received {len(gdf)} features")
        else:
            logging.error(f"Error: {response.status_code}")
            gdf = gpd.GeoDataFrame(columns=["geometry", "id"])

        return gdf

    def search(self, pagination={}, provider_properties={}, **kwargs) -> gpd.GeoDataFrame:
        """Implements the Boson Search endpoint."""
        logger.info("Making request to API.")
        logger.info(f"Search received kwargs: {kwargs}")

        """
        PAGINATION and LIMIT: if limit is None, Boson will page through all results. Set a max
        page size in the __init__ to control the size of each page. If limit is set, the search function
        will return that number of results. Pagination is a dictionary with the keys "page" and "page_size".
        We will pass "page" and "page_size" to the request_features function.
        """
        page = 1
        page_size = self.default_page_size
        limit = kwargs.get("limit", None)
        if limit == 0:
            limit = None
        if limit is not None:
            page_size = limit if limit <= self.default_page_size else self.default_page_size

        if pagination and "page" in pagination and "page_size" in pagination:
            logger.info(f"Received pagination: {pagination}")
            page = pagination.get("page", None)
            if page == 0:
                logger.info("Received page 0. Setting page to 1")
                page = 1

            page_size = pagination.get("page_size", self.default_page_size)

        elif pagination:
            logger.info(f"Received pagination w/o page and/or page_size: {pagination}")
            pagination = {}

        """
        PROVIDER_PROPERTIES: These are the properties set in the boson_config.properties. These are an
        advanced feature and may not be needed for most providers. 
        """
        if provider_properties:
            logger.info(f"Received provider_properties from boson_config.properties: {provider_properties}")
            # TODO: Update kwargs with relevant keys from provider_properties, or otherwise pass them along

        gdf = self.request_features(page=page, page_size=page_size, **kwargs)

        logger.info(f"making pagination dict")
        pagination_dict = {"page": page + 1, "page_size": page_size}
        logger.info(f"pagination_dict: {pagination_dict}")

        logger.info("returning search")
        logger.info(f"Returning gdf of type: {type(gdf)} with length {len(gdf)}")
        logger.info(gdf.head())
        logger.info(f"datatypes of columns are: {gdf.dtypes}")
        # Reset all
        logger.info("Resetting all by running __init__")
        self.__init__()
        return gdf, pagination_dict

    def queryables(self, **kwargs) -> dict:
        """
        Update this method to return a dictionary of queryable parameters that the API accepts.
        The keys should be the parameter names. The values should be a Property object that follows
        the conventions of JSON Schema.
        """
        return {
            "sort": Property(
                title="sort",
                type="string",
            ),
            "ids": Property(
                title="ids",
                type="string",
            ),
            "filters": Property(
                title="filters",
                type="string",
            ),
            "q": Property(
                title="search_query",
                type="string",
            ),
            "pageNo": Property(
                title="pageNo",
                type="integer",
            ),
            "pageSize": Property(
                title="pageSize",
                type="integer",
            ),
            "nextPageToken": Property(
                title="nextPageToken",
                type="string",
            ),
            "previousPageToken": Property(
                title="previousPageToken",
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
            "fields": Property(
                title="fields",
                type="string",
            ),
        }


janes_installations = JanesInstallationsRemoteProvider()
app = serve(search_func=janes_installations.search, queryables_func=janes_installations.queryables)
