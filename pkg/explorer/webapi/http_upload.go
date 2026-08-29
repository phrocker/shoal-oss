// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package webapi

import (
	"errors"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func ingestEndpoint(service Service) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Shoal-Workspace-Request") != "1" {
			writeError(writer, shoal.NewError(
				shoal.ErrorUnauthorized, "workspace request header is required"))
			return
		}
		provider, ok := service.(IngestProvider)
		if !ok {
			writeError(writer, shoal.NewError(
				shoal.ErrorUnavailable, "workspace capability \"ingest\" is unavailable"))
			return
		}
		capabilities, err := capabilitiesFor(request.Context(), service)
		if err != nil {
			writeError(writer, publicIngestError(err))
			return
		}
		if !capabilities.Supports(CapabilityIngest) {
			writeError(writer, shoal.NewError(
				shoal.ErrorUnavailable, "workspace capability \"ingest\" is unavailable"))
			return
		}
		input, err := decodeUploadRequest(writer, request)
		if err != nil {
			writeError(writer, err)
			return
		}
		response, err := provider.Ingest(request.Context(), input)
		if err != nil {
			writeError(writer, publicIngestError(err))
			return
		}
		writeResponse(writer, http.StatusOK, response)
	}
}

func publicIngestError(err error) error {
	code := primaryErrorCode(err)
	if code == shoal.ErrorInvalidArgument && publicUploadValidationError(err) {
		return err
	}
	switch code {
	case shoal.ErrorInternal, shoal.ErrorUnavailable:
		log.Printf("shoal explorer upload failed: %v", err)
	default:
		log.Printf("shoal explorer upload rejected: %v", err)
	}
	return shoal.NewError(code, "upload failed")
}

func publicUploadValidationError(err error) bool {
	var shoalErr *shoal.Error
	if !errors.As(err, &shoalErr) {
		return false
	}
	switch shoalErr.Message {
	case "at least one upload file is required",
		"upload file count exceeds the server bound",
		"upload filenames must be unique",
		"upload file exceeds the server bound",
		"upload request exceeds the server bound",
		"upload file must be valid UTF-8",
		"upload file must be textual UTF-8",
		"upload media type is not supported",
		"upload filename is not allowed":
		return true
	}
	return strings.HasPrefix(
		shoalErr.Message, "upload media type is not supported for extension ")
}

func decodeUploadRequest(
	writer http.ResponseWriter, request *http.Request,
) (IngestRequest, error) {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" {
		return IngestRequest{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "content type must be multipart/form-data")
	}
	request.Body = http.MaxBytesReader(
		writer, request.Body, int64(MaxUploadTotalBytes))
	reader, err := request.MultipartReader()
	if err != nil {
		return IngestRequest{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "decode upload request body")
	}
	files := make([]UploadFile, 0, MaxUploadFiles)
	var total uint64
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return IngestRequest{}, uploadReadError(err)
		}
		file, ok, err := readUploadPart(part, &total)
		if err != nil {
			return IngestRequest{}, err
		}
		if !ok {
			continue
		}
		if uint32(len(files)+1) > MaxUploadFiles {
			return IngestRequest{}, shoal.NewError(
				shoal.ErrorInvalidArgument, "upload file count exceeds the server bound")
		}
		files = append(files, file)
	}
	return IngestRequest{Files: files}, nil
}

func readUploadPart(
	part *multipart.Part, total *uint64,
) (UploadFile, bool, error) {
	defer part.Close()
	rawName, ok, err := uploadPartFilename(part)
	if err != nil {
		return UploadFile{}, false, err
	}
	if !ok {
		return UploadFile{}, false, nil
	}
	content, err := io.ReadAll(io.LimitReader(part, int64(MaxUploadFileBytes)+1))
	if err != nil {
		return UploadFile{}, false, uploadReadError(err)
	}
	if uint64(len(content)) > MaxUploadFileBytes {
		return UploadFile{}, false, shoal.NewError(
			shoal.ErrorInvalidArgument, "upload file exceeds the server bound")
	}
	*total += uint64(len(content))
	if *total > MaxUploadTotalBytes {
		return UploadFile{}, false, shoal.NewError(
			shoal.ErrorInvalidArgument, "upload request exceeds the server bound")
	}
	return UploadFile{Name: rawName, Content: content}, true, nil
}

func uploadPartFilename(part *multipart.Part) (string, bool, error) {
	contentDisposition := part.Header.Get("Content-Disposition")
	_, params, err := mime.ParseMediaType(contentDisposition)
	if err != nil {
		return "", false, shoal.NewError(
			shoal.ErrorInvalidArgument, "upload part content disposition is invalid")
	}
	filename := params["filename"]
	if filename == "" {
		return "", false, nil
	}
	return filename, true, nil
}

func uploadReadError(err error) error {
	var maxBytes *http.MaxBytesError
	if errors.As(err, &maxBytes) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "upload request exceeds the server bound")
	}
	return shoal.NewError(shoal.ErrorInvalidArgument, "decode upload request body")
}
