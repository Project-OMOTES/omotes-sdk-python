# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: task.proto
# Protobuf Python Version: 4.25.2
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\ntask.proto\"\xc1\x01\n\nTaskResult\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x16\n\x0e\x63\x65lery_task_id\x18\x02 \x01(\t\x12+\n\x0bresult_type\x18\x03 \x01(\x0e\x32\x16.TaskResult.ResultType\x12\x18\n\x0boutput_esdl\x18\x04 \x01(\x0cH\x00\x88\x01\x01\x12\x0c\n\x04logs\x18\x05 \x01(\t\"&\n\nResultType\x12\r\n\tSUCCEEDED\x10\x00\x12\t\n\x05\x45RROR\x10\x01\x42\x0e\n\x0c_output_esdl\"_\n\x12TaskProgressUpdate\x12\x0e\n\x06job_id\x18\x01 \x01(\t\x12\x16\n\x0e\x63\x65lery_task_id\x18\x02 \x01(\t\x12\x10\n\x08progress\x18\x03 \x01(\x01\x12\x0f\n\x07message\x18\x04 \x01(\tb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'task_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_TASKRESULT']._serialized_start=15
  _globals['_TASKRESULT']._serialized_end=208
  _globals['_TASKRESULT_RESULTTYPE']._serialized_start=154
  _globals['_TASKRESULT_RESULTTYPE']._serialized_end=192
  _globals['_TASKPROGRESSUPDATE']._serialized_start=210
  _globals['_TASKPROGRESSUPDATE']._serialized_end=305
# @@protoc_insertion_point(module_scope)
