from __future__ import annotations
from typing import Any, Dict, List, Tuple
from unittest.mock import patch
import pandas as pd
import pytest
from pv_lakehouse.etl.clients.openelectricity.client import (
    OpenElectricityClient,
    RequestsHTTPClient,
    load_api_key,
)
from pv_lakehouse.etl.clients.openelectricity.models import ClientConfig

class MockHTTPClient:
    def __init__(self, responses: List[Dict[str, Any]] = None):
        self.responses = responses or []
        self.calls: List[Dict[str, Any]] = []
        self._response_index = 0

    def get(
        self,
        url: str,
        headers: Dict[str, str],
        params: List[Tuple[str, str]],
        timeout: int,
    ) -> Dict[str, Any]:
        self.calls.append({
            "url": url,
            "headers": headers,
            "params": params,
            "timeout": timeout,
        })
        if self._response_index < len(self.responses):
            response = self.responses[self._response_index]
            self._response_index += 1
            return response
        return {"success": True, "data": []}


class TestLoadApiKey:
    def test_load_from_parameter(self):
        result = load_api_key("test-key-123")
        assert result == "test-key-123"

    def test_load_from_environment(self):
        with patch.dict("os.environ", {"OPENELECTRICITY_API_KEY": "env-key-456"}):
            result = load_api_key()
            assert result == "env-key-456"

    def test_load_missing_raises_error(self):
        with patch.dict("os.environ", {}, clear=True):
            with patch("pv_lakehouse.etl.clients.openelectricity.client._read_env_file") as mock:
                mock.return_value = {}
                with pytest.raises(RuntimeError, match="Missing API key"):
                    load_api_key()


class TestRequestsHTTPClient:
    def test_get_unauthorized(self):
        client = RequestsHTTPClient()
        with patch("requests.get") as mock_get:
            mock_response = mock_get.return_value
            mock_response.status_code = 401
            mock_response.raise_for_status.side_effect = None

            with pytest.raises(RuntimeError, match="401 Unauthorized"):
                client.get(
                    url="https://api.example.com/test",
                    headers={"Authorization": "Bearer invalid"},
                    params=[],
                    timeout=30,
                )


class TestOpenElectricityClient:
    def test_client_initialization(self):
        client = OpenElectricityClient(api_key="test-key")
        assert client.api_key == "test-key"

    def test_client_with_custom_config(self):
        config = ClientConfig(
            base_url="https://custom.api.com/v1",
            timeout=60,
        )
        client = OpenElectricityClient(api_key="test-key", config=config)
        assert "custom.api.com" in client._facilities_endpoint

    def test_client_with_mock_http_client(self):
        mock_http = MockHTTPClient(responses=[
            {
                "success": True,
                "data": [
                    {
                        "code": "FAC1",
                        "name": "Test Facility",
                        "network_id": "NEM",
                        "network_region": "NSW1",
                        "units": [],
                    }
                ],
            }
        ])

        client = OpenElectricityClient(api_key="test-key", http_client=mock_http)
        df = client.fetch_facilities()

        # Verify API was called
        assert len(mock_http.calls) == 1
        assert "facilities" in mock_http.calls[0]["url"]

        # Verify result
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]["facility_code"] == "FAC1"

    def test_fetch_facilities_with_filters(self):
        mock_http = MockHTTPClient(responses=[{"success": True, "data": []}])

        client = OpenElectricityClient(api_key="test-key", http_client=mock_http)
        client.fetch_facilities(
            networks=["NEM"],
            statuses=["operating"],
            fueltechs=["solar_utility", "solar_rooftop"],
            region="NSW1",
        )

        # Verify parameters were passed correctly
        params = dict(mock_http.calls[0]["params"])
        assert ("network_id", "NEM") in mock_http.calls[0]["params"]
        assert ("status_id", "operating") in mock_http.calls[0]["params"]
        assert ("network_region", "NSW1") in mock_http.calls[0]["params"]

    def test_fetch_facilities_selected_codes(self):
        mock_http = MockHTTPClient(responses=[
            {
                "success": True,
                "data": [
                    {"code": "FAC1", "name": "Facility 1", "network_id": "NEM", "units": []},
                    {"code": "FAC2", "name": "Facility 2", "network_id": "NEM", "units": []},
                    {"code": "FAC3", "name": "Facility 3", "network_id": "NEM", "units": []},
                ],
            }
        ])

        client = OpenElectricityClient(api_key="test-key", http_client=mock_http)
        df = client.fetch_facilities(selected_codes=["FAC1", "FAC3"])

        assert len(df) == 2
        assert set(df["facility_code"]) == {"FAC1", "FAC3"}

    def test_fetch_facilities_empty_response(self):
        mock_http = MockHTTPClient(responses=[{"success": True, "data": []}])

        client = OpenElectricityClient(api_key="test-key", http_client=mock_http)
        df = client.fetch_facilities()

        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_fetch_timeseries_invalid_interval(self):
        client = OpenElectricityClient(api_key="test-key")

        with pytest.raises(ValueError, match="Invalid interval"):
            client.fetch_timeseries(
                facility_codes=["FAC1"],
                interval="invalid",
            )

    def test_fetch_timeseries_date_validation(self):
        mock_http = MockHTTPClient()
        client = OpenElectricityClient(api_key="test-key", http_client=mock_http)

        with pytest.raises(ValueError, match="Both date_start and date_end"):
            client.fetch_timeseries(
                facility_codes=["FAC1"],
                date_start="2024-01-01T00:00:00",
            )

        # Verify no API calls were made
        assert len(mock_http.calls) == 0

        # date_end before date_start - should fail validation before any API call
        with pytest.raises(ValueError, match="date_end must be after date_start"):
            client.fetch_timeseries(
                facility_codes=["FAC1"],
                date_start="2024-01-31T00:00:00",
                date_end="2024-01-01T00:00:00",
            )

        # Verify still no API calls were made
        assert len(mock_http.calls) == 0

    def test_headers_include_bearer_token(self):
        mock_http = MockHTTPClient(responses=[{"success": True, "data": []}])

        client = OpenElectricityClient(api_key="my-secret-key", http_client=mock_http)
        client.fetch_facilities()

        headers = mock_http.calls[0]["headers"]
        assert headers["Authorization"] == "Bearer my-secret-key"
        assert headers["Accept"] == "application/json"


class TestBackwardCompatibility:
    def test_fetch_facilities_dataframe_function(self):
        from pv_lakehouse.etl.clients.openelectricity import fetch_facilities_dataframe

        with patch.object(OpenElectricityClient, "fetch_facilities") as mock:
            mock.return_value = pd.DataFrame()
            result = fetch_facilities_dataframe(api_key="test-key")
            assert isinstance(result, pd.DataFrame)

    def test_fetch_facility_timeseries_dataframe_function(self):
        from pv_lakehouse.etl.clients.openelectricity import fetch_facility_timeseries_dataframe

        with patch.object(OpenElectricityClient, "fetch_timeseries") as mock:
            mock.return_value = pd.DataFrame()
            result = fetch_facility_timeseries_dataframe(
                api_key="test-key",
                facility_codes=["FAC1"],
            )
            assert isinstance(result, pd.DataFrame)
