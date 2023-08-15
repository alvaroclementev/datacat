"""Configuration management for datacat"""
from __future__ import annotations

import argparse
from collections import ChainMap
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
    field_name: str = "timestamp"


def compile(config_path: Path, args: argparse.Namespace) -> Configuration:
    """Load the configuration from the different sources and merge it.

    The configuration is loaded from the following sources (with this priority):
        - CLI arguments
        - env variables (TODO)
        - Config file
    """
    # TODO(alvaro): Can we provide sane defaults to that this just works? Maybe
    # require a path to a file and that's it (default to csv, now, no delay, json,
    # console)
    # TODO(alvaro): Error handling and reporting
    args_data = prepare_cli_args(args)
    file_data = prepare_config_file(config_path)

    merged_data = ChainMap(args_data, file_data)

    return Configuration.model_validate(merged_data)


def prepare_config_file(path: Path) -> dict:
    """Loads the config file"""
    # FIXME(alvaro): We probably want to allow for this file to not exist if there are
    # enough arguments
    if not path.exists():
        raise RuntimeError(f"the configuration file does not exist: {path}")

    # Load the data
    with path.open("r") as f:
        data = yaml.safe_load(f)
    return data


def prepare_cli_args(args: argparse.Namespace) -> dict:
    """Takes the arguments from the CLI and prepares a dictionary with overrides"""

    data = {}

    # Validate the path
    if args.path is not None and not args.path.exists():
        raise RuntimeError("data path not found")

    if args.path is not None:
        data["source"] = {
            "type": "csv",
            "path": str(args.path),
        }

    return data
