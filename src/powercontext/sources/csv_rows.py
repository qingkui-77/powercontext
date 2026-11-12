# Copyright (c) 2026 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Captured CSV rows with stable document and primary-key identities."""

from __future__ import annotations

import csv
import io
import json
from typing import Literal
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, Field, model_validator

from powercontext.sources.definitions import SourceProjection
from powercontext.sources.models import Source, SourceMaterialization
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY, TextEvidence

CSV_ROW_SOURCE_NAME = "csv-row"
CSV_ROW_SOURCE_VERSION = "1"
MAX_CSV_ROW_SOURCE_ID_LENGTH = 256


class CSVDocumentInput(BaseModel):
    """Complete byte content for one offline CSV document."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    document_id: str
    primary_key: str
    content: bytes


class CSVRowInput(BaseModel):
    """One parsed CSV row, retaining the header and values in source order."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    document_id: str
    primary_key: str
    primary_key_value: str
    columns: tuple[str, ...]
    values: tuple[str, ...]

    @model_validator(mode="after")
    def validate_row_shape(self) -> CSVRowInput:
        if not self.document_id or self.document_id != self.document_id.strip():
            raise ValueError("document_id must be non-empty and trimmed")  # noqa: TRY003
        if not self.primary_key or not self.primary_key.strip() or self.primary_key not in self.columns:
            raise ValueError("primary_key must exactly match a column")  # noqa: TRY003
        if not self.primary_key_value or not self.primary_key_value.strip():
            raise ValueError("primary_key_value must not be blank")  # noqa: TRY003
        if not self.columns or len(self.columns) != len(self.values):
            raise ValueError("columns and values must have the same non-zero width")  # noqa: TRY003
        if (
            any(not column or not column.strip() for column in self.columns)
            or len(set(self.columns)) != len(self.columns)
        ):
            raise ValueError("columns must be non-blank and unique")  # noqa: TRY003
        return self


class CSVRowSource(Source):
    """Persistable captured Source describing one exact CSV row."""

    source_id: str
    document_id: str
    primary_key: str
    primary_key_value: str
    columns: tuple[str, ...]
    values: tuple[str, ...]
    materialization: Literal[SourceMaterialization.CAPTURED] = SourceMaterialization.CAPTURED

    @model_validator(mode="after")
    def validate_identity_and_row(self) -> CSVRowSource:
        expected = csv_row_source_id(self.document_id, self.primary_key_value)
        if self.source_id != expected or self.name != expected:
            raise ValueError("name and source_id must match the CSV row identity")  # noqa: TRY003
        if not self.primary_key or not self.primary_key.strip() or self.primary_key not in self.columns:
            raise ValueError("primary_key must exactly match a column")  # noqa: TRY003
        if len(self.columns) != len(self.values) or not self.columns:
            raise ValueError("columns and values must have the same non-zero width")  # noqa: TRY003
        if (
            any(not column or not column.strip() for column in self.columns)
            or len(set(self.columns)) != len(self.columns)
        ):
            raise ValueError("columns must be non-blank and unique")  # noqa: TRY003
        key_index = self.columns.index(self.primary_key)
        if self.values[key_index] != self.primary_key_value:
            raise ValueError("primary_key_value must match the row value")  # noqa: TRY003
        return self


def csv_row_source_id(document_id: str, primary_key_value: str, /) -> str:
    """Build the unambiguous stable ID for a document and exact key value."""

    if not isinstance(document_id, str) or not document_id or document_id != document_id.strip():
        raise ValueError("document_id must be non-empty and trimmed")  # noqa: TRY003
    if not isinstance(primary_key_value, str) or not primary_key_value or not primary_key_value.strip():
        raise ValueError("primary_key_value must not be blank")  # noqa: TRY003
    source_id = f"csv:{quote(document_id, safe='')}:{quote(primary_key_value, safe='')}"
    if len(source_id) > MAX_CSV_ROW_SOURCE_ID_LENGTH:
        raise ValueError(f"source_id must not exceed {MAX_CSV_ROW_SOURCE_ID_LENGTH} characters")  # noqa: TRY003
    return source_id


def parse_csv_document(value: CSVDocumentInput, /) -> tuple[CSVRowInput, ...]:
    """Decode, parse, and atomically validate a complete UTF-8 CSV document."""

    if not isinstance(value, CSVDocumentInput):
        raise ValueError("value must be a CSVDocumentInput")  # noqa: TRY003
    if not value.document_id or value.document_id != value.document_id.strip():
        raise ValueError("document_id must be non-empty and trimmed")  # noqa: TRY003
    if not value.primary_key or not value.primary_key.strip():
        raise ValueError("primary_key must be non-empty")  # noqa: TRY003

    try:
        text = value.content.decode("utf-8-sig", errors="strict")
        reader = csv.reader(io.StringIO(text, newline=""), delimiter=",", quotechar='"', doublequote=True, strict=True)
        columns = next(reader)
    except (UnicodeDecodeError, csv.Error, StopIteration) as error:
        raise ValueError("invalid or empty CSV document") from error  # noqa: TRY003

    if not columns or any(not column or not column.strip() for column in columns):
        raise ValueError("CSV header fields must be non-empty and non-blank")  # noqa: TRY003
    if len(set(columns)) != len(columns):
        raise ValueError("CSV header fields must be unique")  # noqa: TRY003
    if value.primary_key not in columns:
        raise ValueError("primary_key must exactly match a CSV header field")  # noqa: TRY003

    key_index = columns.index(value.primary_key)
    rows: list[CSVRowInput] = []
    seen_keys: set[str] = set()
    try:
        for fields in reader:
            if not fields:  # A completely empty physical record.
                continue
            if len(fields) != len(columns):
                raise ValueError("CSV data row width must match the header")  # noqa: TRY003
            key_value = fields[key_index]
            if not key_value or not key_value.strip():
                raise ValueError("CSV primary key values must not be blank")  # noqa: TRY003
            if key_value in seen_keys:
                raise ValueError("CSV primary key values must be unique")  # noqa: TRY003
            csv_row_source_id(value.document_id, key_value)
            seen_keys.add(key_value)
            rows.append(
                CSVRowInput(
                    document_id=value.document_id,
                    primary_key=value.primary_key,
                    primary_key_value=key_value,
                    columns=tuple(columns),
                    values=tuple(fields),
                )
            )
    except csv.Error as error:
        raise ValueError("malformed CSV document") from error  # noqa: TRY003
    return tuple(rows)


class CSVRowSourceDefinition:
    """Resolve row inputs and expose canonical TextEvidence projections."""

    name = CSV_ROW_SOURCE_NAME
    version = CSV_ROW_SOURCE_VERSION
    input_class = CSVRowInput
    source_class = CSVRowSource
    projections: tuple[SourceProjection[CSVRowSource], ...]

    def __init__(self) -> None:
        self.projections = (CSVRowTextEvidenceProjection(),)

    async def resolve(self, value: CSVRowInput, /) -> CSVRowSource:
        source_id = csv_row_source_id(value.document_id, value.primary_key_value)
        return CSVRowSource(
            name=source_id,
            source_id=source_id,
            definition_version=self.version,
            materialization=SourceMaterialization.CAPTURED,
            document_id=value.document_id,
            primary_key=value.primary_key,
            primary_key_value=value.primary_key_value,
            columns=value.columns,
            values=value.values,
        )

    async def read(self, source: CSVRowSource, /) -> CSVRowInput:
        return CSVRowInput(
            document_id=source.document_id,
            primary_key=source.primary_key,
            primary_key_value=source.primary_key_value,
            columns=source.columns,
            values=source.values,
        )


class CSVRowTextEvidenceProjection:
    """Project the complete ordered row without changing any field values."""

    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class = CSVRowSource
    output_class: type[BaseModel] = TextEvidence

    def project(self, source: CSVRowSource, /) -> TextEvidence:
        row = dict(zip(source.columns, source.values, strict=True))
        return TextEvidence(
            source_type=CSV_ROW_SOURCE_NAME,
            source_id=source.source_id,
            content=json.dumps(row, ensure_ascii=False, separators=(",", ":")),
            metadata={
                "document_id": source.document_id,
                "primary_key": source.primary_key,
                "primary_key_value": source.primary_key_value,
                "columns": list(source.columns),
                "row": row,
            },
        )


CSV_ROW_SOURCE_DEFINITION = CSVRowSourceDefinition()


__all__ = [
    "CSV_ROW_SOURCE_DEFINITION",
    "CSVDocumentInput",
    "CSVRowInput",
    "CSVRowSource",
    "CSVRowSourceDefinition",
    "CSVRowTextEvidenceProjection",
    "csv_row_source_id",
    "parse_csv_document",
]
