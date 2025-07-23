import uuid
from datetime import UTC, datetime

from pydantic import BaseModel
from sqlmodel import Field, SQLModel


class QueryRecordBase(SQLModel):
    query: str
    status: str
    submitted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class QueryRecord(QueryRecordBase, table=True):
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    completed_at: datetime | None = None
    result_file: str | None = None
    error: str | None = None


class QueryRecordCreate(QueryRecordBase):
    pass


class QueryRecordUpdate(SQLModel):
    status: str
    error: str | None = None
    completed_at: datetime | None = None
    result_file: str | None = None


class RedshiftDatabase(BaseModel):
    """Information about a database in a Redshift cluster.

    Based on the SVV_REDSHIFT_DATABASES system view.
    """

    database_name: str = Field(..., description="The name of the database")
    database_owner: int | None = Field(None, description="The database owner user ID")
    database_type: str | None = Field(
        None, description="The type of database (local or shared)"
    )
    database_acl: str | None = Field(
        None, description="Access control information (for internal use)"
    )
    database_options: str | None = Field(
        None, description="The properties of the database"
    )
    database_isolation_level: str | None = Field(
        None,
        description="The isolation level of the database (Snapshot Isolation or Serializable)",
    )


class RedshiftSchema(BaseModel):
    """Information about a schema in a Redshift database.

    Based on the SVV_ALL_SCHEMAS system view.
    """

    database_name: str = Field(
        ..., description="The name of the database where the schema exists"
    )
    schema_name: str = Field(..., description="The name of the schema")
    schema_owner: int | None = Field(
        None, description="The user ID of the schema owner"
    )
    schema_type: str | None = Field(
        None, description="The type of the schema (external, local, or shared)"
    )
    schema_acl: str | None = Field(
        None,
        description="The permissions for the specified user or user group for the schema",
    )
    source_database: str | None = Field(
        None, description="The name of the source database for external schema"
    )
    schema_option: str | None = Field(
        None, description="The options of the schema (external schema attribute)"
    )


class RedshiftTable(BaseModel):
    """Information about a table in a Redshift database.

    Based on the SVV_ALL_TABLES system view.
    """

    database_name: str = Field(
        ..., description="The name of the database where the table exists"
    )
    schema_name: str = Field(..., description="The schema name for the table")
    table_name: str = Field(..., description="The name of the table")
    table_acl: str | None = Field(
        None,
        description="The permissions for the specified user or user group for the table",
    )
    table_type: str | None = Field(
        None,
        description="The type of the table (views, base tables, external tables, shared tables)",
    )
    remarks: str | None = Field(None, description="Remarks about the table")


class RedshiftColumn(BaseModel):
    """Information about a column in a Redshift table.

    Based on the SVV_ALL_COLUMNS system view.
    """

    database_name: str = Field(..., description="The name of the database")
    schema_name: str = Field(..., description="The name of the schema")
    table_name: str = Field(..., description="The name of the table")
    column_name: str = Field(..., description="The name of the column")
    ordinal_position: int | None = Field(
        None, description="The position of the column in the table"
    )
    column_default: str | None = Field(
        None, description="The default value of the column"
    )
    is_nullable: str | None = Field(
        None, description="Whether the column is nullable (yes or no)"
    )
    data_type: str | None = Field(None, description="The data type of the column")
    character_maximum_length: int | None = Field(
        None, description="The maximum number of characters in the column"
    )
    numeric_precision: int | None = Field(None, description="The numeric precision")
    numeric_scale: int | None = Field(None, description="The numeric scale")
    remarks: str | None = Field(None, description="Remarks about the column")
