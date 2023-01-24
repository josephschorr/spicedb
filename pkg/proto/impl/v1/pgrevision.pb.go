// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        (unknown)
// source: impl/v1/pgrevision.proto

package implv1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// PostgresRevision is a compact binary encoding of a postgres snapshot as
// described in the offial documentation here:
// https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-PG-SNAPSHOT-PARTS
//
// We use relative offsets for xmax and the xips to reduce the number of bytes
// required for binary encoding using the protobuf varint datatype:
// https://protobuf.dev/programming-guides/encoding/#varints
type PostgresRevision struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Xmin         uint64  `protobuf:"varint,1,opt,name=xmin,proto3" json:"xmin,omitempty"`
	RelativeXmax int64   `protobuf:"varint,2,opt,name=relative_xmax,json=relativeXmax,proto3" json:"relative_xmax,omitempty"`
	RelativeXips []int64 `protobuf:"varint,3,rep,packed,name=relative_xips,json=relativeXips,proto3" json:"relative_xips,omitempty"`
}

func (x *PostgresRevision) Reset() {
	*x = PostgresRevision{}
	if protoimpl.UnsafeEnabled {
		mi := &file_impl_v1_pgrevision_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PostgresRevision) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PostgresRevision) ProtoMessage() {}

func (x *PostgresRevision) ProtoReflect() protoreflect.Message {
	mi := &file_impl_v1_pgrevision_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PostgresRevision.ProtoReflect.Descriptor instead.
func (*PostgresRevision) Descriptor() ([]byte, []int) {
	return file_impl_v1_pgrevision_proto_rawDescGZIP(), []int{0}
}

func (x *PostgresRevision) GetXmin() uint64 {
	if x != nil {
		return x.Xmin
	}
	return 0
}

func (x *PostgresRevision) GetRelativeXmax() int64 {
	if x != nil {
		return x.RelativeXmax
	}
	return 0
}

func (x *PostgresRevision) GetRelativeXips() []int64 {
	if x != nil {
		return x.RelativeXips
	}
	return nil
}

var File_impl_v1_pgrevision_proto protoreflect.FileDescriptor

var file_impl_v1_pgrevision_proto_rawDesc = []byte{
	0x0a, 0x18, 0x69, 0x6d, 0x70, 0x6c, 0x2f, 0x76, 0x31, 0x2f, 0x70, 0x67, 0x72, 0x65, 0x76, 0x69,
	0x73, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x07, 0x69, 0x6d, 0x70, 0x6c,
	0x2e, 0x76, 0x31, 0x22, 0x70, 0x0a, 0x10, 0x50, 0x6f, 0x73, 0x74, 0x67, 0x72, 0x65, 0x73, 0x52,
	0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x12, 0x0a, 0x04, 0x78, 0x6d, 0x69, 0x6e, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x04, 0x78, 0x6d, 0x69, 0x6e, 0x12, 0x23, 0x0a, 0x0d, 0x72,
	0x65, 0x6c, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x78, 0x6d, 0x61, 0x78, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x0c, 0x72, 0x65, 0x6c, 0x61, 0x74, 0x69, 0x76, 0x65, 0x58, 0x6d, 0x61, 0x78,
	0x12, 0x23, 0x0a, 0x0d, 0x72, 0x65, 0x6c, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x78, 0x69, 0x70,
	0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x03, 0x52, 0x0c, 0x72, 0x65, 0x6c, 0x61, 0x74, 0x69, 0x76,
	0x65, 0x58, 0x69, 0x70, 0x73, 0x42, 0x90, 0x01, 0x0a, 0x0b, 0x63, 0x6f, 0x6d, 0x2e, 0x69, 0x6d,
	0x70, 0x6c, 0x2e, 0x76, 0x31, 0x42, 0x0f, 0x50, 0x67, 0x72, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f,
	0x6e, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x01, 0x5a, 0x33, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62,
	0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x75, 0x74, 0x68, 0x7a, 0x65, 0x64, 0x2f, 0x73, 0x70, 0x69,
	0x63, 0x65, 0x64, 0x62, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x69,
	0x6d, 0x70, 0x6c, 0x2f, 0x76, 0x31, 0x3b, 0x69, 0x6d, 0x70, 0x6c, 0x76, 0x31, 0xa2, 0x02, 0x03,
	0x49, 0x58, 0x58, 0xaa, 0x02, 0x07, 0x49, 0x6d, 0x70, 0x6c, 0x2e, 0x56, 0x31, 0xca, 0x02, 0x07,
	0x49, 0x6d, 0x70, 0x6c, 0x5c, 0x56, 0x31, 0xe2, 0x02, 0x13, 0x49, 0x6d, 0x70, 0x6c, 0x5c, 0x56,
	0x31, 0x5c, 0x47, 0x50, 0x42, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0xea, 0x02, 0x08,
	0x49, 0x6d, 0x70, 0x6c, 0x3a, 0x3a, 0x56, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_impl_v1_pgrevision_proto_rawDescOnce sync.Once
	file_impl_v1_pgrevision_proto_rawDescData = file_impl_v1_pgrevision_proto_rawDesc
)

func file_impl_v1_pgrevision_proto_rawDescGZIP() []byte {
	file_impl_v1_pgrevision_proto_rawDescOnce.Do(func() {
		file_impl_v1_pgrevision_proto_rawDescData = protoimpl.X.CompressGZIP(file_impl_v1_pgrevision_proto_rawDescData)
	})
	return file_impl_v1_pgrevision_proto_rawDescData
}

var file_impl_v1_pgrevision_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_impl_v1_pgrevision_proto_goTypes = []interface{}{
	(*PostgresRevision)(nil), // 0: impl.v1.PostgresRevision
}
var file_impl_v1_pgrevision_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_impl_v1_pgrevision_proto_init() }
func file_impl_v1_pgrevision_proto_init() {
	if File_impl_v1_pgrevision_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_impl_v1_pgrevision_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PostgresRevision); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_impl_v1_pgrevision_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_impl_v1_pgrevision_proto_goTypes,
		DependencyIndexes: file_impl_v1_pgrevision_proto_depIdxs,
		MessageInfos:      file_impl_v1_pgrevision_proto_msgTypes,
	}.Build()
	File_impl_v1_pgrevision_proto = out.File
	file_impl_v1_pgrevision_proto_rawDesc = nil
	file_impl_v1_pgrevision_proto_goTypes = nil
	file_impl_v1_pgrevision_proto_depIdxs = nil
}