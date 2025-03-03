//nolint:wrapcheck
package main

import (
	"fmt"
	"io"
	"os"
	"sort"

	descriptor "github.com/golang/protobuf/protoc-gen-go/descriptor"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

const generatedFileWarning = "// Code generated by pachgen (etc/proto/pachgen). DO NOT EDIT.\n"

// Generate:
// Unsupported transaction client (overridden by user code) - existing in client/transaction.go

// ProtoCodeGenerator is an interface to be implemented by each type of output
// file we are generating.  'AddProto' is called for each input .proto file, and
// then 'Finish' is called to collect the resulting generated .go file.
type ProtoCodeGenerator interface {
	AddProto(*descriptor.FileDescriptorProto) error
	Finish() (*plugin.CodeGeneratorResponse_File, error)
}

// CodeGenerators is the set of all code generators that will be run for any
// input files.  It is populated by 'init' in each code generator's
// implementation.
var CodeGenerators = []ProtoCodeGenerator{}

// errResponse is a helper function to easily construct an error response to send to protoc.
func errResponse(err error) *plugin.CodeGeneratorResponse {
	errString := err.Error()
	return &plugin.CodeGeneratorResponse{Error: &errString}
}

type fdProtoSlice []*descriptorpb.FileDescriptorProto

func (s fdProtoSlice) Len() int           { return len(s) }
func (s fdProtoSlice) Less(i, j int) bool { return s[i].GetName() < s[j].GetName() }
func (s fdProtoSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type serviceSlice []*descriptorpb.ServiceDescriptorProto

func (s serviceSlice) Len() int           { return len(s) }
func (s serviceSlice) Less(i, j int) bool { return s[i].GetName() < s[j].GetName() }
func (s serviceSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type methodSlice []*descriptorpb.MethodDescriptorProto

func (s methodSlice) Len() int           { return len(s) }
func (s methodSlice) Less(i, j int) bool { return s[i].GetName() < s[j].GetName() }
func (s methodSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func sortProtos(pp []*descriptorpb.FileDescriptorProto) {
	sort.Sort(fdProtoSlice(pp))
	for _, p := range pp {
		sort.Sort(serviceSlice(p.GetService()))
		for _, s := range p.GetService() {
			sort.Sort(methodSlice(s.GetMethod()))
		}
	}
}

// runInternal will send each proto file to each code generator, then collect
// their output files into a single response.
func runInternal(req *plugin.CodeGeneratorRequest) *plugin.CodeGeneratorResponse {
	sortProtos(req.ProtoFile)
	for _, proto := range req.ProtoFile {
		for _, gen := range CodeGenerators {
			if err := gen.AddProto(proto); err != nil {
				return errResponse(err)
			}
		}
	}

	resp := &plugin.CodeGeneratorResponse{}
	for _, gen := range CodeGenerators {
		if respFile, err := gen.Finish(); err != nil {
			fmt.Fprintf(os.Stderr, "error from codegen: %v\n", err)
			// return errResponse(err)
		} else {
			// Add the generated file warning here rather than count on the generators to do it
			newContent := generatedFileWarning + *respFile.Content
			respFile.Content = &newContent
			resp.File = append(resp.File, respFile)
		}
	}

	return resp
}

// run handles the input/output of communicating with 'protoc', reading a
// request from stdin and writing a response to stdout.
func run() error {
	req := &plugin.CodeGeneratorRequest{}
	if data, err := io.ReadAll(os.Stdin); err != nil {
		return err
	} else if err := proto.Unmarshal(data, req); err != nil {
		return err
	}

	resp := runInternal(req)

	if marshalled, err := proto.Marshal(resp); err != nil {
		return err
	} else if _, err := os.Stdout.Write(marshalled); err != nil {
		return err
	} else if resp.Error != nil {
		return fmt.Errorf("%s", *resp.Error)
	}

	return nil
}

// main wraps 'run', which returns an error if we should have a non-zero exit code
func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %v\n", err)
		os.Exit(1)
	}
}
