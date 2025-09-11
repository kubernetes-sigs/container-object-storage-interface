/*
If this program is on the path of your machine you can invoke it in the following way:

protoc --plugin protoc-gen-gofake --goexample_out=package=cosi,packagePath=sigs.k8s.io/container-object-storage-interface-spec:fake cosi.proto

Requires package, and packagePath to be specified
*/
package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	. "github.com/dave/jennifer/jen"
	"google.golang.org/protobuf/proto"
	plugin "google.golang.org/protobuf/types/pluginpb"
)

type GoFake struct {
	Request    *plugin.CodeGeneratorRequest
	Response   *plugin.CodeGeneratorResponse
	Parameters map[string]string
}

type FakeService struct {
	Name    string
	Methods []Method
}

type Method struct {
	Name   string
	Input  string
	Output string
}

func cleanInput(val string) string {
	spl := strings.Split(val, ".")
	return spl[len(spl)-1]
}

func (runner *GoFake) getLocationMessage() map[string][]*FakeService {
	ret := make(map[string][]*FakeService)
	for _, protoFile := range runner.Request.ProtoFile {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", *protoFile.Name)
		fakeServices := make([]*FakeService, 0)
		_, _ = fmt.Fprintf(os.Stderr, "%+v\n", protoFile.GetService())
		svcs := protoFile.GetService()
		for _, svc := range svcs {
			_, _ = fmt.Fprintf(os.Stderr, "service: %+v\n", svc)
			current := &FakeService{
				Name: fmt.Sprintf("%sClient", *svc.Name),
			}
			methods := make([]Method, 0)
			for _, mtd := range svc.Method {
				method := Method{Name: *mtd.Name}
				if mtd.InputType != nil {
					method.Input = cleanInput(*mtd.InputType)
				}
				if mtd.OutputType != nil {
					method.Output = cleanInput(*mtd.OutputType)
				}
				methods = append(methods, method)
			}
			current.Methods = methods

			fakeServices = append(fakeServices, current)
		}
		ret[*protoFile.Name] = fakeServices
	}
	return ret
}

func (runner *GoFake) CreateFakeFile(filename string, fakeSVC []*FakeService) error {
	var outfileName string
	var content string
	outfileName = strings.Replace(filename, ".proto", ".pb.fake.go", -1)
	var mdFile plugin.CodeGeneratorResponse_File
	mdFile.Name = &outfileName

	pkgPath := runner.Parameters["packagePath"]

	f := NewFile("fake")
	for _, fakeSVC := range fakeSVC {
		s := f.Type().Id(fmt.Sprintf("Fake%s", fakeSVC.Name))
		fakeMethds := make([]Code, len(fakeSVC.Methods))
		for i, mtd := range fakeSVC.Methods {
			fakeMethds[i] = Id(fmt.Sprintf("Fake%s", mtd.Name)).Func().Params(
				Id("ctx").Qual("context", "Context"),
				Id("in").Op("*").Qual(pkgPath, mtd.Input),
				Id("opts").Op("...").Qual("google.golang.org/grpc", "CallOption"),
			).Op("(").List(
				Op("*").Qual(pkgPath, mtd.Output),
				Error(),
			).Op(")")
		}
		s.Struct(fakeMethds...)
		for _, mtd := range fakeSVC.Methods {
			f.Func().Params(Id("f").Id(fmt.Sprintf("*Fake%s", fakeSVC.Name))).Id(mtd.Name).Params(
				Id("ctx").Qual("context", "Context"),
				Id("in").Op("*").Qual(pkgPath, mtd.Input),
				Id("opts").Op("...").Qual("google.golang.org/grpc", "CallOption"),
			).Op("(").List(
				Op("*").Qual(pkgPath, mtd.Output),
				Error(),
			).Op(")").Block(
				Return(Id("f").Dot(fmt.Sprintf("Fake%s", mtd.Name)).Call(
					Id("ctx"),
					Id("in"),
					Id("opts").Op("..."),
				)),
			)
		}
	}
	content = fmt.Sprintf("%#v", f)
	mdFile.Content = &content
	runner.Response.File = append(runner.Response.File, &mdFile)
	return nil
}

func (runner *GoFake) generateMessageMarkdown() error {
	// This convenience method will return a structure of some types that I use
	for filename, locationMessages := range runner.getLocationMessage() {
		runner.CreateFakeFile(filename, locationMessages)
	}
	return nil
}

func (runner *GoFake) generateCode() error {
	// Initialize the output file slice
	files := make([]*plugin.CodeGeneratorResponse_File, 0)
	runner.Response.File = files

	err := runner.generateMessageMarkdown()
	if err != nil {
		return err
	}
	return nil
}

func main() {
	// os.Stdin will contain data which will unmarshal into the following object:
	// https://godoc.org/github.com/golang/protobuf/protoc-gen-go/plugin#CodeGeneratorRequest
	req := &plugin.CodeGeneratorRequest{}
	resp := &plugin.CodeGeneratorResponse{}

	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		panic(err)
	}

	// You must use the requests unmarshal method to handle this type
	if err := proto.Unmarshal(data, req); err != nil {
		panic(err)
	}

	// You may require more data than what is in the proto files alone.  There are a couple ways in which to do this.
	// The first is by parameters.  Another may be using leading comments in the proto files which I will cover in generateCode.
	parameters := req.GetParameter()
	// =grpc,import_path=mypackage:.
	exampleRunner := &GoFake{
		Request:    req,
		Response:   resp,
		Parameters: make(map[string]string),
	}
	groupkv := strings.Split(parameters, ",")
	for _, element := range groupkv {
		kv := strings.Split(element, "=")
		if len(kv) > 1 {
			exampleRunner.Parameters[kv[0]] = kv[1]
		}
	}

	err = exampleRunner.generateCode()
	if err != nil {
		panic(err)
	}

	marshalled, err := proto.Marshal(resp)
	if err != nil {
		panic(err)
	}
	os.Stdout.Write(marshalled)
}
