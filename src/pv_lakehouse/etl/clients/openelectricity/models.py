from __future__ import annotations #Cho phép sử dụng kiểu dữ liệu lồng nhau (ví dụ: List[Unit]) ngay trong class định nghĩa
from datetime import datetime
from typing import Any, Dict, List, Optional #định nghĩa kiểu dữ liệu cho biến, tham số hàm
from pydantic import BaseModel, Field, field_validator #validation dữ liệu, tạo data models với type checking tự động
from pyspark.sql import types as T #định nghĩa schema cho DataFrame

#Spark schema
FACILITY_SCHEMA = T.StructType(
    [
        T.StructField("facility_code", T.StringType(), True),
        T.StructField("facility_name", T.StringType(), True),
        T.StructField("network_id", T.StringType(), True),
        T.StructField("network_region", T.StringType(), True),
        T.StructField("facility_created_at", T.StringType(), True),
        T.StructField("facility_updated_at", T.StringType(), True),
        T.StructField("location_lat", T.DoubleType(), True),
        T.StructField("location_lng", T.DoubleType(), True),
        T.StructField("unit_count", T.IntegerType(), True),
        T.StructField("total_capacity_mw", T.DoubleType(), True),
        T.StructField("total_capacity_registered_mw", T.DoubleType(), True),
        T.StructField("total_capacity_maximum_mw", T.DoubleType(), True),
        T.StructField("total_capacity_storage_mwh", T.DoubleType(), True),
        T.StructField("unit_fueltech_summary", T.StringType(), True),
        T.StructField("unit_status_summary", T.StringType(), True),
        T.StructField("unit_dispatch_summary", T.StringType(), True),
        T.StructField("unit_codes", T.StringType(), True),
        T.StructField("facility_description", T.StringType(), True),
    ]
)

#API response models
#Data model cho vị trí địa lý
class Location(BaseModel):
    lat: Optional[float] = None #Kinh độ
    lng: Optional[float] = None #Vĩ độ

#Data model cho đơn vị điện
class Unit(BaseModel):
    code: Optional[str] = None  #Mã đơn vị
    name: Optional[str] = None #Tên đơn vị
    status_id: Optional[str] = Field(None, alias="status") #Trạng thái
    fueltech_id: Optional[str] = Field(None, alias="fueltech") #Loại nhiên liệu
    dispatch_type: Optional[str] = None #Loại dispatch
    capacity_mw: Optional[float] = Field(None, alias="capacity") #Dung lượng
    capacity_registered: Optional[float] = None #Dung lượng đăng ký
    capacity_maximum: Optional[float] = None #Dung lượng tối đa
    capacity_storage: Optional[float] = None #Dung lượng lưu trữ

    model_config = {"populate_by_name": True, "extra": "allow"} 

    @field_validator("capacity_mw", "capacity_registered", "capacity_maximum", "capacity_storage", mode="before")
    @classmethod #Chuyển đổi giá trị thành float
    def coerce_float(cls, v: Any) -> Optional[float]:
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

#Data model cho trạm
class Facility(BaseModel):
    code: Optional[str] = None #Mã trạm
    name: Optional[str] = None #Tên trạm
    network_id: Optional[str] = None #Mã mạng lưới
    network_region: Optional[str] = None #Khu vực mạng lưới
    created_at: Optional[str] = None #Ngày tạo
    updated_at: Optional[str] = None #Ngày cập nhật
    description: Optional[str] = None #Mô tả
    location: Optional[Location] = None #Vị trí
    units: List[Unit] = Field(default_factory=list) #Danh sách đơn vị

    model_config = {"extra": "allow"}

#Data model cho metadata của trạm
class FacilityMetadata(BaseModel):
    code: str #Mã trạm
    name: Optional[str] = None #Tên trạm
    network_id: str #Mã mạng lưới
    network_region: Optional[str] = None #Khu vực mạng lưới
    units: List[Dict[str, Any]] = Field(default_factory=list) #Danh sách đơn vị

#Data model cho response của trạm
class FacilitiesResponse(BaseModel):
    success: bool = False #Kết quả
    data: List[Facility] = Field(default_factory=list) #Danh sách trạm
    error: Optional[str] = None #Lỗi

#Data model cho response của timeseries
class TimeseriesColumns(BaseModel):
    unit_code: Optional[str] = None #Mã đơn vị
    facility_code: Optional[str] = None #Mã trạm

class TimeseriesResult(BaseModel):
    columns: Optional[TimeseriesColumns] = None #Cột
    data: List[List[Any]] = Field(default_factory=list) 
class TimeseriesSeries(BaseModel):
    network_code: Optional[str] = None #Mã mạng lưới
    metric: Optional[str] = None #Đơn vị đo
    interval: Optional[str] = None #Khoảng thời gian
    unit: Optional[str] = None #Đơn vị
    results: List[TimeseriesResult] = Field(default_factory=list) 

    model_config = {"extra": "allow"}

class TimeseriesResponse(BaseModel):
    success: bool = False #Kết quả
    data: List[TimeseriesSeries] = Field(default_factory=list) 
    error: Optional[str] = None #Lỗi

# Facility summary model
class FacilitySummary(BaseModel):
    facility_code: Optional[str] = None #Mã trạm
    facility_name: Optional[str] = None #Tên trạm
    network_id: Optional[str] = None #Mã mạng lưới
    network_region: Optional[str] = None #Khu vực mạng lưới
    facility_created_at: Optional[str] = None #Ngày tạo
    facility_updated_at: Optional[str] = None #Ngày cập nhật
    location_lat: Optional[float] = None #Vĩ độ
    location_lng: Optional[float] = None #Kinh độ
    unit_count: int = 0 #Số lượng đơn vị
    total_capacity_mw: Optional[float] = None #Tổng dung lượng
    total_capacity_registered_mw: Optional[float] = None #Tổng dung lượng đăng ký
    total_capacity_maximum_mw: Optional[float] = None #Tổng dung lượng tối đa
    total_capacity_storage_mwh: Optional[float] = None #Tổng dung lượng lưu trữ
    unit_fueltech_summary: Optional[str] = None #Tóm tắt loại nhiên liệu
    unit_status_summary: Optional[str] = None #Tóm tắt trạng thái
    unit_dispatch_summary: Optional[str] = None #Tóm tắt loại dispatch
    unit_codes: Optional[str] = None #Mã đơn vị
    facility_description: Optional[str] = None #Mô tả

class TimeseriesRow(BaseModel):
    network_code: Optional[str] = None #Mã mạng lưới
    network_id: Optional[str] = None #Mã mạng lưới
    network_region: Optional[str] = None #Khu vực mạng lưới
    facility_code: Optional[str] = None #Mã trạm
    facility_name: Optional[str] = None #Tên trạm
    unit_code: Optional[str] = None #Mã đơn vị
    metric: Optional[str] = None #Đơn vị đo
    interval: Optional[str] = None #Khoảng thời gian
    value_unit: Optional[str] = None #Đơn vị
    interval_start: Optional[str] = None #Thời gian bắt đầu
    value: Optional[float] = None #Giá trị

class ClientConfig(BaseModel):
    base_url: str = "https://api.openelectricity.org.au/v4"
    timeout: int = 120
    max_retries: int = 3
    retry_min_wait: float = 1.0  # seconds
    retry_max_wait: float = 60.0  # seconds

class DateRange(BaseModel):
    start: datetime
    end: datetime

    @field_validator("end")
    @classmethod #Kiểm tra end phải sau start
    def end_after_start(cls, v: datetime, info) -> datetime:
        if "start" in info.data and v <= info.data["start"]:
            raise ValueError("end must be after start")
        return v


__all__ = [
    # Spark schema
    "FACILITY_SCHEMA",
    # API response models
    "Location",
    "Unit",
    "Facility",
    "FacilityMetadata",
    "FacilitiesResponse",
    "TimeseriesColumns",
    "TimeseriesResult",
    "TimeseriesSeries",
    "TimeseriesResponse",
    # Output models
    "FacilitySummary",
    "TimeseriesRow",
    # Config models
    "ClientConfig",
    "DateRange",
]