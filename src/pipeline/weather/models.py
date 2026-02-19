from __future__ import annotations
from pydantic import BaseModel, ConfigDict, Field
from typing import Optional, Any
from datetime import datetime

# The main observation model that captures the properties of a weather observation.
class QuantifiedValue(BaseModel):
    model_config = ConfigDict(extra='ignore', populate_by_name=True)
    unit_code: Optional[str] = Field(None, alias="unitCode", description="The unit of measurement for the temperature value.")
    value: Optional[float] = Field(None, description="The temperature value.")
    quality_control: Optional[str] = Field(None, alias="qualityControl", description="Quality control flag for the temperature value.")

class Elevation(BaseModel):
    model_config = ConfigDict(extra='ignore', populate_by_name=True)
    unit_code: Optional[str] = Field(None, alias="unitCode", description="The unit of measurement for the elevation value.")
    value: Optional[float] = Field(None, description="The elevation value.")

class Geometry(BaseModel):
    model_config = ConfigDict(extra='ignore', populate_by_name=True)
    type: str = Field(..., description="The type of geometry (e.g., 'Point').")
    coordinates: list[float] = Field(..., description="The coordinates of the geometry.")

class CloudLayer(BaseModel):
    model_config = ConfigDict(extra='ignore', populate_by_name=True)
    base: Optional[Elevation] = Field(None, description="The base elevation of the cloud layer.")
    amount: Optional[str] = Field(None, description="The amount of cloud cover")

class ObservationProperties(BaseModel):
    model_config = ConfigDict(extra='ignore', populate_by_name=True)

    station_id: str = Field(..., alias="stationId", description="The unique identifier for the weather station.")
    station_name: str = Field(..., alias="stationName", description="The name of the weather station.")
    timestamp: datetime = Field(..., description="The timestamp of the observation.")
    elevation: Optional[Elevation] = Field(None, description="The elevation information for the station.")
    text_description: Optional[str] = Field(None, alias="textDescription", description="A textual description of the weather conditions.")
    icon: Optional[str] = Field(None, description="A URL to an icon representing the weather conditions.")
    present_weather: list[Any] = Field(default_factory=list, alias="presentWeather", description="A list of present weather conditions.")
    temperature: Optional[QuantifiedValue] = Field(None, description="The temperature observation.")
    wind_direction: Optional[QuantifiedValue] = Field(None, alias="windDirection", description="The wind direction observation.")
    wind_speed: Optional[QuantifiedValue] = Field(None, alias="windSpeed", description="The wind speed observation.")
    wind_gust: Optional[QuantifiedValue] = Field(None, alias="windGust", description="The wind gust observation.")
    visibility: Optional[QuantifiedValue] = Field(None, description="The visibility observation.")
    precipitation_last_hour: Optional[QuantifiedValue] = Field(None, alias="precipitationLastHour", description="The precipitation in the last hour observation.")
    precipitation_last_3_hours: Optional[QuantifiedValue] = Field(None, alias="precipitationLast3Hours", description="The precipitation in the last 3 hours observation.")
    precipitation_last_6_hours: Optional[QuantifiedValue] = Field(None, alias="precipitationLast6Hours", description="The precipitation in the last 6 hours observation.")
    cloud_layers: Optional[list[CloudLayer]] = Field(None, alias="cloudLayers", description="A list of cloud layers observed.")

class NwsStationObservation(BaseModel):
    model_config = ConfigDict(extra='ignore', populate_by_name=True)
    id: str = Field(..., description="The unique identifier for the observation.")
    type: str = Field(..., description="The type of the observation (e.g., 'Feature').")
    geometry: Geometry = Field(..., description="The geometry information for the observation.")
    properties: ObservationProperties = Field(..., description="Payload from NWS API")
# END NWS Classes

#Begin Beat To Station Mapping

class BeatStationMapping(BaseModel):
    model_config = ConfigDict(extra='ignore', populate_by_name=True)
    object_id: int = Field(..., alias="objectid")
    beat: int = Field(..., description='Beat ID for police beat')
    name: str = Field(..., description='Name of Police Beat')
    representative_lat: float = Field(..., description="Best latitude to represent beat")
    representative_lon: float = Field(..., description="Best longitute to represent beat")
    station_id: str = Field(..., description="Station Id, used to query NWS API")
