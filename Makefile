#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

SHELL := /bin/bash

# Pinned to the exact Apache Thrift compiler version Accumulo's pom uses
# (version.thrift = 0.17.0). DO NOT use the system thrift — versions may drift.
THRIFT       ?= thrift
THRIFT_IDL   ?= $(ACCUMULO_SRC)/core/src/main/thrift
THRIFT_OUT   := internal/thrift/gen

# Subset of Accumulo's Thrift IDL shoal needs (read path only).
THRIFT_FILES := \
	$(THRIFT_IDL)/tabletscan.thrift \
	$(THRIFT_IDL)/data.thrift \
	$(THRIFT_IDL)/security.thrift \
	$(THRIFT_IDL)/client.thrift

.PHONY: all
all: build

.PHONY: thrift-check
thrift-check:
	@test -x $(THRIFT) || { echo "thrift compiler not found at $(THRIFT)"; exit 1; }
	@v=$$($(THRIFT) --version | awk '{print $$3}'); \
	  test "$$v" = "0.17.0" || { echo "expected thrift 0.17.0, got $$v"; exit 1; }

.PHONY: thrift-gen
thrift-gen: thrift-check
	rm -rf $(THRIFT_OUT)
	mkdir -p $(THRIFT_OUT)
	for f in $(THRIFT_FILES); do \
	  $(THRIFT) -r --gen go:package_prefix=github.com/accumulo/shoal/internal/thrift/gen/ \
	    -out $(THRIFT_OUT) $$f || exit 1; \
	done
	# Drop standalone -remote debug CLIs; they target a newer apache/thrift
	# Go runtime API and we don't use them.
	find $(THRIFT_OUT) -type d -name '*-remote' -exec rm -rf {} +
	$(MAKE) patch-thrift-nil-binary

# Patch generated writeFieldN functions that write a struct-pointer field
# unconditionally. Java treats absent struct fields as "no value"; the Go
# generator emits an empty struct in their place, which Accumulo's server
# typically interprets as "configured but malformed" → NPE.
#
# We do this generically: any writeField that contains `p.<Field>.Write(ctx, oprot)`
# gets a `if p.<Field> == nil { return nil }` guard inserted at the top.
# Same logic for binary fields (`p.<Field>` passed to WriteBinary) — we
# only guard the explicitly-listed ones we know need it (TKeyExtent's
# endRow/prevEndRow), since not all empty binary fields are equivalent
# to absent on the Java side.
.PHONY: patch-thrift-nil-binary
patch-thrift-nil-binary:
	@$(MAKE) --no-print-directory _patch-binary-fields
	@$(MAKE) --no-print-directory _patch-struct-fields

# Patch the two known empty-binary-as-absent fields (TKeyExtent.endRow / prevEndRow).
.PHONY: _patch-binary-fields
_patch-binary-fields:
	@f=$(THRIFT_OUT)/data/data.go; \
	test -f $$f || { echo "$$f not found; run thrift-gen first"; exit 1; }; \
	if grep -q 'PATCH (shoal): skip endRow' $$f; then \
	  echo "_patch-binary-fields: already applied"; \
	else \
	  awk 'BEGIN { p=0 } \
	       /^func \(p \*TKeyExtent\) writeField2\b/ { p=1; print; next } \
	       /^func \(p \*TKeyExtent\) writeField3\b/ { p=2; print; next } \
	       p==1 && /WriteFieldBegin\(ctx, "endRow"/     { print "  // PATCH (shoal): skip endRow when nil so wire matches Java\047s \"infinite endRow\" semantics."; print "  if p.EndRow == nil { return nil }"; p=0 } \
	       p==2 && /WriteFieldBegin\(ctx, "prevEndRow"/ { print "  // PATCH (shoal): null prevEndRow = \"infinite prev\" (start of table)."; print "  if p.PrevEndRow == nil { return nil }"; p=0 } \
	       { print }' $$f > $$f.tmp && mv $$f.tmp $$f; \
	  echo "_patch-binary-fields: applied"; \
	fi

# Generic struct-pointer guard: any writeFieldN across all generated .go
# files where the body contains `p.<Field>.Write(ctx, oprot)` gets the
# nil-skip guard injected. Idempotent: the marker comment prevents re-patching.
.PHONY: _patch-struct-fields
_patch-struct-fields:
	@find $(THRIFT_OUT) -name '*.go' -print0 | xargs -0 -I{} sh -c '\
	  f="$$1"; \
	  awk -v file="$$f" '"'"' \
	    BEGIN { changed=0 } \
	    /^func \(p \*[A-Za-z0-9_]+\) writeField[0-9]+\(ctx context\.Context, oprot thrift\.TProtocol\) \(err error\) \{$$/ { \
	      header=$$0; getline next1; getline next2; getline next3; \
	      if (next3 ~ /p\.[A-Z][A-Za-z0-9_]*\.Write\(ctx, oprot\)/ && next1 !~ /PATCH \(shoal\)/) { \
	        match(next3, /p\.([A-Z][A-Za-z0-9_]*)\.Write/, m); \
	        print header; \
	        print "  // PATCH (shoal): null struct-pointer field = absent on wire."; \
	        print "  if p." m[1] " == nil { return nil }"; \
	        print next1; print next2; print next3; \
	        changed=1; next; \
	      } else { print header; print next1; print next2; print next3; next; } \
	    } \
	    { print } \
	    END { if (changed) print "_patch-struct-fields: patched " file > "/dev/stderr" } \
	  '"'"' "$$f" > "$$f.tmp" && mv "$$f.tmp" "$$f"; \
	' _ {}

.PHONY: build
build:
	go build ./...

.PHONY: test
test:
	go test ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: clean
clean:
	rm -rf $(THRIFT_OUT) bin
