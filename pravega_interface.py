"""utilities for working with pravega"""
# to be used from jython
import contextlib
import uuid


from java.net import URI
from io.pravega.client import ClientConfig
from io.pravega.client.stream import Stream
from io.pravega.client.admin import StreamManager
from io.pravega.client.admin import ReaderGroupManager
from io.pravega.client.stream import ReaderConfig
from io.pravega.client.stream import ReaderGroupConfig
from io.pravega.client import EventStreamClientFactory
from io.pravega.client.stream import EventStreamReader
from io.pravega.client.stream import ReaderGroupConfig
from io.pravega.client.stream import ScalingPolicy
from io.pravega.client.stream import StreamConfiguration

from io.pravega.client.stream import EventStreamWriter
from io.pravega.client.stream import EventWriterConfig

from io.pravega.client.admin import KeyValueTableManager
from io.pravega.client.tables import (
    KeyValueTableClientConfiguration,
    KeyValueTableConfiguration,
)
from io.pravega.client import KeyValueTableFactory


@contextlib.contextmanager
def streamManager(uri):
    """return a StreamManager context for the specified uri"""
    try:
        stream_manager = StreamManager.create(URI(uri))
        yield stream_manager
    finally:
        if stream_manager:
            stream_manager.close()


def streamConfiguration(scaling_policy=1):
    """return a stream configuration object"""
    stream_config = StreamConfiguration.builder()
    if scaling_policy:
        stream_config.scalingPolicy(ScalingPolicy.fixed(scaling_policy))
    return stream_config.build()


@contextlib.contextmanager
def eventStreamClientFactory(uri, scope):
    """create an EventStreamClientFactory"""
    clientFactory = None
    try:
        clientFactory = EventStreamClientFactory.withScope(
            scope, ClientConfig.builder().controllerURI(URI(uri)).build()
        )
        yield clientFactory
    finally:
        if clientFactory:
            clientFactory.close()


@contextlib.contextmanager
def readerGroupManager(uri, scope):
    """return a ReaderGroupManager context"""
    try:
        reader_group_manager = ReaderGroupManager.withScope(scope, URI(uri))
        yield reader_group_manager
    finally:
        if reader_group_manager:
            reader_group_manager.close()


@contextlib.contextmanager
def readerGroup(reader_group_manager, scope, stream_name, reader_group_name=None):
    """return a ReaderGroup context"""
    reader_group_config = (
        ReaderGroupConfig.builder().stream(Stream.of(scope, stream_name)).build()
    )
    if reader_group_name is None:
        reader_group_name = str(uuid.uuid4()).replace("-", "")
    try:
        reader_group = None
        reader_group_manager.createReaderGroup(reader_group_name, reader_group_config)
        reader_group = reader_group_manager.getReaderGroup(reader_group_name)
        yield reader_group
    finally:
        if reader_group:
            reader_group.close()


@contextlib.contextmanager
def Reader(reader_group, clientFactory, serializer, reader_name="reader"):
    """create a Reader in specified group"""
    reader = None
    try:
        reader = clientFactory.createReader(
            reader_name,
            reader_group.getGroupName(),
            serializer,
            ReaderConfig.builder().build(),
        )
        yield reader
    finally:
        if reader:
            reader.close()


@contextlib.contextmanager
def eventWriter(clientFactory, stream_name, serializer):
    """create an event writer"""
    event_writer = None
    try:
        event_writer = clientFactory.createEventWriter(
            stream_name, serializer, EventWriterConfig.builder().build()
        )
        yield event_writer
    finally:
        if event_writer:
            event_writer.close()


@contextlib.contextmanager
def keyValueTableManager(uri):
    """create a kvt table manager"""
    key_value_table_manager = None
    try:
        key_value_table_manager = KeyValueTableManager.create(URI(uri))
        yield key_value_table_manager
    finally:
        if key_value_table_manager:
            key_value_table_manager.close()


def keyValueTableConfiguration(partition_count=1):
    """return a kvt configuration"""
    key_value_table_configuration = KeyValueTableConfiguration.builder()
    if partition_count:
        key_value_table_configuration.partitionCount(partition_count)

    return key_value_table_configuration.build()


@contextlib.contextmanager
def keyValueTableFactory(uri, scope):
    """create a KeyValueTableFactory"""
    kvt_factory = None
    try:
        kvt_factory = KeyValueTableFactory.withScope(
            scope, ClientConfig.builder().controllerURI(URI(uri)).build()
        )
        yield kvt_factory
    finally:
        if kvt_factory:
            kvt_factory.close()


@contextlib.contextmanager
def keyValueTable(kvt_factory, table_name, key_serializer, value_serializer):
    """create a kvt"""
    kvt_table = None
    try:
        kvt_table = kvt_factory.forKeyValueTable(
            table_name,
            key_serializer,
            value_serializer,
            KeyValueTableClientConfiguration.builder().build(),
        )
        yield kvt_table
    finally:
        if kvt_table:
            kvt_table.close()
