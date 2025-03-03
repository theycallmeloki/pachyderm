// Code generated by protoc-gen-zap (etc/proto/protoc-gen-zap). DO NOT EDIT.
//
// source: server/worker/pipeline/transform/transform.proto

package transform

import (
	zapcore "go.uber.org/zap/zapcore"
)

func (x *CreateParallelDatumsTask) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if x == nil {
		return nil
	}
	enc.AddObject("job", x.Job)
	enc.AddString("salt", x.Salt)
	enc.AddString("file_set_id", x.FileSetId)
	enc.AddString("base_file_set_id", x.BaseFileSetId)
	enc.AddObject("path_range", x.PathRange)
	return nil
}

func (x *CreateParallelDatumsTaskResult) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if x == nil {
		return nil
	}
	enc.AddString("file_set_id", x.FileSetId)
	enc.AddObject("stats", x.Stats)
	return nil
}

func (x *CreateSerialDatumsTask) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if x == nil {
		return nil
	}
	enc.AddObject("job", x.Job)
	enc.AddString("salt", x.Salt)
	enc.AddString("file_set_id", x.FileSetId)
	enc.AddObject("base_meta_commit", x.BaseMetaCommit)
	enc.AddBool("no_skip", x.NoSkip)
	enc.AddObject("path_range", x.PathRange)
	return nil
}

func (x *CreateSerialDatumsTaskResult) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if x == nil {
		return nil
	}
	enc.AddString("file_set_id", x.FileSetId)
	enc.AddString("output_delete_file_set_id", x.OutputDeleteFileSetId)
	enc.AddString("meta_delete_file_set_id", x.MetaDeleteFileSetId)
	enc.AddObject("stats", x.Stats)
	return nil
}

func (x *CreateDatumSetsTask) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if x == nil {
		return nil
	}
	enc.AddString("file_set_id", x.FileSetId)
	enc.AddObject("path_range", x.PathRange)
	enc.AddObject("set_spec", x.SetSpec)
	return nil
}

func (x *CreateDatumSetsTaskResult) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if x == nil {
		return nil
	}
	datum_setsArrMarshaller := func(enc zapcore.ArrayEncoder) error {
		for _, v := range x.DatumSets {
			enc.AppendObject(v)
		}
		return nil
	}
	enc.AddArray("datum_sets", zapcore.ArrayMarshalerFunc(datum_setsArrMarshaller))
	return nil
}

func (x *DatumSetTask) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if x == nil {
		return nil
	}
	enc.AddObject("job", x.Job)
	enc.AddString("file_set_id", x.FileSetId)
	enc.AddObject("path_range", x.PathRange)
	enc.AddObject("output_commit", x.OutputCommit)
	return nil
}

func (x *DatumSetTaskResult) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if x == nil {
		return nil
	}
	enc.AddString("output_file_set_id", x.OutputFileSetId)
	enc.AddString("meta_file_set_id", x.MetaFileSetId)
	enc.AddObject("stats", x.Stats)
	return nil
}
