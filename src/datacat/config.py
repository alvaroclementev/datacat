"""Configuration management for datacat"""
from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, FilePath, PositiveFloat


class Configuration(BaseModel):
    """Top level configuration."""

    # To add a new configuration kind (e.g: a new `Sink` with custom arguments), create
    # A new `XXXSourceConfig` with a `type` field with a unique (for that kind)
    # `Literal` value and add it as a union in the corresponding field

    source: CsvSourceConfig = Field(discriminator="type")
    sink: ConsoleSinkConfig = Field(discriminator="type")
    format: JsonSerializerConfig = Field(discriminator="type")
    conductor: FixedRateConductorConfig = Field(discriminator="type")
    timestamp: NowTimestamperConfig = Field(discriminator="type")


class CsvSourceConfig(BaseModel):
    type: Literal["csv"]
    path: FilePath


class ConsoleSinkConfig(BaseModel):
    type: Literal["console"]


class JsonSerializerConfig(BaseModel):
    type: Literal["json"]


class FixedRateConductorConfig(BaseModel):
    type: Literal["rate"]
    rate: PositiveFloat


class NowTimestamperConfig(BaseModel):
    type: Literal["now"]


def load(config_path: Path) -> Configuration:
    with config_path.open("r") as f:
        data = yaml.safe_load(f)

    # TODO(alvaro): Error handling and reporting
    return Configuration.model_validate(data)
