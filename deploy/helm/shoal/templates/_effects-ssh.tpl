{{- /*
effects-ssh (#450, #662): each entry of effectsSsh deep-merged over
effectsSshDefaults, as a JSON list, the same way shoal.effectsGateways merges
the gateways, so validate.yaml and the templates read the same values.
Disabled entries are included; the templates skip them.
*/ -}}
{{- define "shoal.effectsSsh" -}}
{{- $merged := list -}}
{{- $entries := default (list) .Values.effectsSsh -}}
{{- range $index, $entry := (ternary $entries (list) (kindIs "slice" $entries)) -}}
{{- if kindIs "map" $entry -}}
{{- $ssh := mergeOverwrite (deepCopy $.Values.effectsSshDefaults) (deepCopy $entry) -}}
{{- $_ := set $ssh "_index" $index -}}
{{- $merged = append $merged $ssh -}}
{{- end -}}
{{- end -}}
{{- toJson $merged -}}
{{- end -}}

{{- /*
Names. The stem is the effects gateways' 24-character stem, and validate.yaml
holds an entry name to 24 characters:

  stem 24 + "-ssh-" 5 + name 24  -> 54: Deployment, Service, ServiceAccount,
                                         NetworkPolicy
  + "-journal" 8                  -> 62: the PersistentVolumeClaim
  + "-commands" 9                 -> 63: the ConfigMap
  + "-init" 5                     -> 59: the journal-init Job

Takes a dict of root and ssh.
*/ -}}
{{- define "shoal.effectsSshName" -}}
{{- printf "%s-ssh-%s" (include "shoal.effectsGatewayStem" .root) .ssh.name -}}
{{- end -}}

{{- /* The selector: distinct from every other component's, and per entry. */ -}}
{{- define "shoal.effectsSshSelectorLabels" -}}
app.kubernetes.io/name: shoal-effects-ssh
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: effects-ssh
shoal.effects-ssh/name: {{ .ssh.name | quote }}
{{- end -}}

{{- /* The enabled effectsSsh entry whose gateway is the given gateway entry,
       as JSON {selector, port}, or "" when there is none. The gateway's
       NetworkPolicy reaches such a target by pod selector rather than by
       CIDR. Takes a dict of root and gateway. */ -}}
{{- define "shoal.effectsSshForGateway" -}}
{{- $root := .root -}}
{{- $gateway := .gateway -}}
{{- range $ssh := fromJsonArray (include "shoal.effectsSsh" $root) -}}
{{- if and (eq $ssh.enabled true) (eq (toString $ssh.gateway) (toString $gateway.name)) -}}
{{- toJson (dict "selector" (include "shoal.effectsSshSelectorLabels" (dict "root" $root "ssh" $ssh)) "port" (include "shoal.effectsGatewayWhole" (dict "name" "port" "value" $ssh.port))) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- /* The URL the gateway entry must target. Takes a dict of root and ssh. */ -}}
{{- define "shoal.effectsSshBaseURL" -}}
{{- printf "https://%s.%s.svc:%s" (include "shoal.effectsSshName" .) .root.Release.Namespace (include "shoal.effectsGatewayWhole" (dict "name" "port" "value" .ssh.port)) -}}
{{- end -}}

{{- /*
The plan for one entry: every refusal the binary would make at startup, moved
to render time, plus the deployment rules it cannot see (#662), and the values
the templates render, computed once. validate.yaml includes it for every
enabled entry, and so does each template that renders one, so a refusal
fires whichever renders first and a template never renders a value the plan
did not check.

Takes a dict of root and ssh. Returns JSON.
*/ -}}
{{- define "shoal.effectsSshPlan" -}}
{{- $root := .root -}}
{{- $ssh := .ssh -}}
{{- $key := printf "effectsSsh[%v]" $ssh._index -}}
{{- $name := include "shoal.effectsSshName" (dict "root" $root "ssh" $ssh) -}}
{{- $plan := dict "name" $name -}}


{{- /* At most one replica, as for the gateway: the journal is one file on a
       ReadWriteOnce volume, held under flock. A second replica is a second
       journal (and a command run twice) or a process refused on the lock. */ -}}
{{- if not (has (toString $ssh.replicas) (list "0" "1")) -}}
{{- fail (printf "%s.replicas must be 0 or 1 (got %v): the journal that makes each command run at most once is one file on a ReadWriteOnce volume, so a second replica is either a second journal that has never seen the first one's keys, and runs their commands again, or a process refused on the journal's lock" $key $ssh.replicas) -}}
{{- end -}}
{{- range $field := list "image.repository" "image.tag" "tls.secretName" "sshKeys.secretName" "gateway" -}}
{{- $parts := splitList "." $field -}}
{{- $value := index $ssh (first $parts) -}}
{{- if gt (len $parts) 1 -}}
{{- $value = index (default (dict) $value) (last $parts) -}}
{{- end -}}
{{- if not (trim (toString (default "" $value))) -}}
{{- fail (printf "%s.%s is required: see effectsSshDefaults in values.yaml and docs/effects-ssh.md, In the chart" $key $field) -}}
{{- end -}}
{{- end -}}
{{- include "shoal.requireShape" (dict "name" (printf "%s.image.pullPolicy" $key) "value" $ssh.image.pullPolicy "what" "one of Always, IfNotPresent or Never" "pattern" `^(Always|IfNotPresent|Never)$` "why" "the API server accepts no other value") -}}
{{- $port := include "shoal.effectsGatewayWhole" (dict "name" (printf "%s.port" $key) "value" $ssh.port) -}}
{{- if or (not $port) (lt (int64 $port) 1024) (gt (int64 $port) 65535) -}}
{{- fail (printf "%s.port must be 1024 to 65535 (got %v): the pod runs as uid 65532 with every capability dropped" $key $ssh.port) -}}
{{- end -}}
{{- $_ := set $plan "port" (int64 $port) -}}
{{- if not (kindIs "bool" $ssh.networkPolicy.enabled) -}}
{{- fail (printf "%s.networkPolicy.enabled must be a boolean (got %v)" $key $ssh.networkPolicy.enabled) -}}
{{- end -}}

{{- /* The gateway entry this is the target of. */ -}}
{{- $gateway := dict -}}
{{- range $candidate := fromJsonArray (include "shoal.effectsGateways" $root) -}}
{{- if eq (toString $candidate.name) (toString $ssh.gateway) -}}
{{- $gateway = $candidate -}}
{{- end -}}
{{- end -}}
{{- if not $gateway -}}
{{- fail (printf "%s.gateway %q names no entry of effectsGateways: effects-ssh is the HTTP target of one effects gateway, which performs the claim, the fence and the completion; without it nothing calls this service" $key (toString $ssh.gateway)) -}}
{{- end -}}
{{- if not $gateway.enabled -}}
{{- fail (printf "%s.gateway %q is not enabled: nothing would call this service" $key (toString $ssh.gateway)) -}}
{{- end -}}
{{- $gatewayKey := printf "effectsGateways[%v]" $gateway._index -}}
{{- $_ := set $plan "gatewayIndex" $gateway._index -}}
{{- $_ := set $plan "gatewaySelector" (include "shoal.effectsGatewaySelectorLabels" (dict "root" $root "gateway" $gateway)) -}}
{{- $target := $gateway.target -}}
{{- $baseURL := include "shoal.effectsSshBaseURL" (dict "root" $root "ssh" $ssh) -}}
{{- $_ := set $plan "baseURL" $baseURL -}}
{{- if ne (toString $target.baseURL) $baseURL -}}
{{- fail (printf "%s.target.baseURL is %q, and effectsSsh[%v] (%s) is served at %q: the gateway must be pointed at this entry's Service, by exactly that URL, or the two are not one deployment" $gatewayKey (toString $target.baseURL) $ssh._index $ssh.name $baseURL) -}}
{{- end -}}
{{- if not $target.caSecretName -}}
{{- fail (printf "%s.target.caSecretName is required with effectsSsh[%v]: the gateway verifies the target's certificate, which an in-cluster CA issues for %s; with only the system roots every request fails at the handshake while both pods report ready" $gatewayKey $ssh._index $baseURL) -}}
{{- end -}}
{{- if not $target.idempotencyHeader -}}
{{- fail (printf "%s.target.idempotencyHeader is required with effectsSsh[%v]: the journal is keyed by the action's ExecutorKey, which the gateway sends in that header" $gatewayKey $ssh._index) -}}
{{- end -}}
{{- $_ := set $plan "idempotencyHeader" (toString $target.idempotencyHeader) -}}

{{- /* Caller authentication: a token, a pinned client certificate, or both.
       Each must have its other half on the gateway, or every request is
       refused while both pods report ready. */ -}}
{{- $token := $ssh.callerToken -}}
{{- $clientAuth := $ssh.clientAuth -}}
{{- $identities := default (list) $clientAuth.identities -}}
{{- if not (kindIs "slice" $identities) -}}
{{- fail (printf "%s.clientAuth.identities must be a list" $key) -}}
{{- end -}}
{{- if and (not $token.secretName) (not $clientAuth.caSecretName) -}}
{{- fail (printf "%s needs callerToken.secretName, clientAuth.caSecretName, or both: there is no unauthenticated mode, and the binary refuses to start without one" $key) -}}
{{- end -}}
{{- if $token.secretName -}}
{{- if not $token.secretKey -}}
{{- fail (printf "%s.callerToken.secretKey is required when callerToken.secretName is set" $key) -}}
{{- end -}}
{{- if not $target.credentialSecretName -}}
{{- fail (printf "%s.target.credentialSecretName is required: effectsSsh[%v] requires a caller token (callerToken.secretName), and the gateway presents it from that Secret as \"Bearer <token>\"" $gatewayKey $ssh._index) -}}
{{- end -}}
{{- end -}}
{{- if $clientAuth.caSecretName -}}
{{- if not $clientAuth.caSecretKey -}}
{{- fail (printf "%s.clientAuth.caSecretKey is required when clientAuth.caSecretName is set" $key) -}}
{{- end -}}
{{- if not $identities -}}
{{- fail (printf "%s.clientAuth.identities needs at least one pin with clientAuth.caSecretName: a CA alone admits every certificate it issues, and the binary refuses it" $key) -}}
{{- end -}}
{{- if gt (len $identities) 16 -}}
{{- fail (printf "%s.clientAuth.identities holds %d pins, over the 16 the binary accepts" $key (len $identities)) -}}
{{- end -}}
{{- range $index, $identity := $identities -}}
{{- if not (regexMatch `^(uri:[A-Za-z][A-Za-z0-9+.-]*:[!-~]+|dns:[A-Za-z0-9][A-Za-z0-9.-]*|spki-sha256:([0-9a-fA-F]{64}|[A-Za-z0-9+/]{43}=))$` (toString $identity)) -}}
{{- fail (printf "%s.clientAuth.identities[%d] %q must be uri:<URI SAN>, dns:<DNS name, no wildcard> or spki-sha256:<64 hex digits or 44 base64 characters>" $key $index (toString $identity)) -}}
{{- end -}}
{{- end -}}
{{- if not (dig "clientCertificate" "secretName" "" $target) -}}
{{- fail (printf "%s.target.clientCertificate.secretName is required: effectsSsh[%v] admits callers by a pinned client certificate (clientAuth), and without one the gateway presents none, so every request is refused" $gatewayKey $ssh._index) -}}
{{- end -}}
{{- else if $identities -}}
{{- fail (printf "%s.clientAuth.identities is set without clientAuth.caSecretName: a pin with nothing to verify the chain against admits nothing, and the binary refuses it" $key) -}}
{{- end -}}

{{- /* The commands. */ -}}
{{- $commands := $ssh.commands -}}
{{- if or (not (kindIs "map" $commands)) (not $commands) -}}
{{- fail (printf "%s.commands needs at least one command: the registry is the only thing this service runs" $key) -}}
{{- end -}}
{{- if gt (len $commands) 256 -}}
{{- fail (printf "%s.commands holds %d commands, over the 256 the binary accepts" $key (len $commands)) -}}
{{- end -}}
{{- $operation := int64 (include "shoal.durationMillis" (dict "name" (printf "%s.timing.operationTimeout" $gatewayKey) "value" $gateway.timing.operationTimeout)) -}}
{{- $allowed := list "address" "hostKeys" "user" "privateKeyKey" "command" "connectTimeout" "runTimeout" "maxOutputBytes" "cidrs" -}}
{{- $registry := dict -}}
{{- $keys := list -}}
{{- $egress := list -}}
{{- $dns := false -}}
{{- $longest := int64 0 -}}
{{- $largestOutput := int64 16384 -}}
{{- range $command, $spec := $commands -}}
{{- $at := printf "%s.commands.%s" $key $command -}}
{{- if not (regexMatch `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$` $command) -}}
{{- fail (printf "%s.commands has a command named %q: names are [A-Za-z0-9][A-Za-z0-9._-]{0,63}, the last segment of the route's path" $key $command) -}}
{{- end -}}
{{- if not (kindIs "map" $spec) -}}
{{- fail (printf "%s must be a map of the command's settings" $at) -}}
{{- end -}}
{{- range $field, $_ := $spec -}}
{{- if not (has $field $allowed) -}}
{{- fail (printf "%s.%s is not a command setting (%s): a misspelled field would be silently ignored, and a missing timeout or bound replaced by its default" $at $field (join ", " $allowed)) -}}
{{- end -}}
{{- end -}}
{{- range $field := list "address" "user" "privateKeyKey" "command" "runTimeout" -}}
{{- if not (trim (toString (default "" (get $spec $field)))) -}}
{{- fail (printf "%s.%s is required" $at $field) -}}
{{- end -}}
{{- end -}}
{{- $address := toString $spec.address -}}
{{- if not (regexMatch `^(\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9][A-Za-z0-9.-]*):[0-9]{1,5}$` $address) -}}
{{- fail (printf "%s.address must be host:port, with an IPv6 address in brackets (got %q)" $at $address) -}}
{{- end -}}
{{- $host := regexReplaceAll `^\[?(.*?)\]?:[0-9]+$` $address "${1}" -}}
{{- $sshPort := int64 (regexFind `[0-9]+$` $address) -}}
{{- if or (lt $sshPort 1) (gt $sshPort 65535) -}}
{{- fail (printf "%s.address port must be 1 to 65535 (got %q)" $at $address) -}}
{{- end -}}
{{- $hostKeys := default (list) $spec.hostKeys -}}
{{- if or (not (kindIs "slice" $hostKeys)) (lt (len $hostKeys) 1) (gt (len $hostKeys) 8) -}}
{{- fail (printf "%s.hostKeys must list 1 to 8 pinned host keys: there is no trust on first use" $at) -}}
{{- end -}}
{{- $keyName := toString $spec.privateKeyKey -}}
{{- if or (gt (len $keyName) 253) (not (regexMatch `^[-._a-zA-Z0-9]+$` $keyName)) (has $keyName (list "." "..")) -}}
{{- fail (printf "%s.privateKeyKey must be a Secret key ([-._a-zA-Z0-9]+, not . or ..; got %q)" $at $keyName) -}}
{{- end -}}
{{- if not (has $keyName $keys) -}}
{{- $keys = append $keys $keyName -}}
{{- end -}}
{{- if gt (len (toString $spec.command)) 4096 -}}
{{- fail (printf "%s.command is over 4096 bytes: a multi-line script belongs in a file on the host" $at) -}}
{{- end -}}
{{- $connectText := toString (default "10s" $spec.connectTimeout) -}}
{{- $connect := int64 (include "shoal.durationMillis" (dict "name" (printf "%s.connectTimeout" $at) "value" $connectText)) -}}
{{- if or (le $connect 0) (gt $connect 60000) -}}
{{- fail (printf "%s.connectTimeout must be positive and at most 60s (got %q)" $at $connectText) -}}
{{- end -}}
{{- $run := int64 (include "shoal.durationMillis" (dict "name" (printf "%s.runTimeout" $at) "value" $spec.runTimeout)) -}}
{{- if or (le $run 0) (gt $run 3600000) -}}
{{- fail (printf "%s.runTimeout must be positive and at most 1h (got %q)" $at (toString $spec.runTimeout)) -}}
{{- end -}}
{{- $budget := add (mul $connect 2) $run 10000 -}}
{{- if ge $budget $operation -}}
{{- fail (printf "%s: 2 × connectTimeout + runTimeout + 10s (%dms) must be below %s.timing.operationTimeout (%q), so the gateway's first attempt gets the answer: connect, the session open, the run, the close and the fsyncs all happen inside that one request" $at $budget $gatewayKey (toString $gateway.timing.operationTimeout)) -}}
{{- end -}}
{{- $longest = max $longest $budget -}}
{{- $entry := dict "address" $address "host_keys" $hostKeys "user" (toString $spec.user) "private_key_file" (printf "/etc/effects-ssh/keys/%s" $keyName) "command" (toString $spec.command) "connect_timeout" $connectText "run_timeout" (toString $spec.runTimeout) -}}
{{- if not (kindIs "invalid" $spec.maxOutputBytes) -}}
{{- $output := include "shoal.effectsGatewayWhole" (dict "name" (printf "%s.maxOutputBytes" $at) "value" $spec.maxOutputBytes) -}}
{{- if or (not $output) (lt (int64 $output) 1) (gt (int64 $output) 262144) -}}
{{- fail (printf "%s.maxOutputBytes must be 1 to 262144 (got %v)" $at $spec.maxOutputBytes) -}}
{{- end -}}
{{- $_ := set $entry "max_output_bytes" (int64 $output) -}}
{{- $largestOutput = max $largestOutput (int64 $output) -}}
{{- end -}}
{{- $_ := set $registry $command $entry -}}
{{- /* Egress for the NetworkPolicy: the address's CIDRs on its port. */ -}}
{{- $literal := regexMatch `^([0-9]{1,3}\.){3}[0-9]{1,3}$|:` $host -}}
{{- if not $literal -}}
{{- $dns = true -}}
{{- end -}}
{{- $cidrs := default (list) $spec.cidrs -}}
{{- if not (kindIs "slice" $cidrs) -}}
{{- fail (printf "%s.cidrs must be a list of CIDRs" $at) -}}
{{- end -}}
{{- if and (not $cidrs) $literal -}}
{{- $cidrs = list (printf "%s/%s" $host (ternary "128" "32" (contains ":" $host))) -}}
{{- end -}}
{{- if and $ssh.networkPolicy.enabled (not $cidrs) -}}
{{- fail (printf "%s.cidrs is required with networkPolicy.enabled and a host name address (%q): a NetworkPolicy matches addresses, not names, so with none listed the command can never connect" $at $address) -}}
{{- end -}}
{{- range $cidr := $cidrs -}}
{{- if not (regexMatch `^([0-9]{1,3}\.){3}[0-9]{1,3}/[0-9]{1,2}$|^[0-9A-Fa-f:]+/[0-9]{1,3}$` (toString $cidr)) -}}
{{- fail (printf "%s.cidrs holds %q, which is not a CIDR" $at (toString $cidr)) -}}
{{- end -}}
{{- if has (toString $cidr) (list "0.0.0.0/0" "::/0") -}}
{{- fail (printf "%s.cidrs holds %q: the policy exists to hold this pod, which holds SSH keys, to the registered hosts. Name the hosts' addresses" $at (toString $cidr)) -}}
{{- end -}}
{{- end -}}
{{- $egress = append $egress (dict "cidrs" $cidrs "port" $sshPort) -}}
{{- end -}}
{{- $_ := set $plan "config" (toJson (dict "commands" $registry)) -}}
{{- $_ := set $plan "keys" (sortAlpha $keys) -}}
{{- $_ := set $plan "egress" $egress -}}
{{- $_ := set $plan "dns" $dns -}}

{{- /* The gateway's routes: each a literal POST to a registered command,
       on idempotency key. A templated path would let the agent's input pick
       any registered command. */ -}}
{{- range $index, $route := (default (list) $gateway.routes) -}}
{{- $path := toString (get $route "path") -}}
{{- $named := trimPrefix "/v1/commands/" $path -}}
{{- if or (not (hasPrefix "/v1/commands/" $path)) (not (hasKey $registry $named)) -}}
{{- fail (printf "%s.routes[%d].path %q must be /v1/commands/<name> for a command in effectsSsh[%v].commands, written literally: a templated path would let the agent's input pick any registered command, and an unregistered one is refused on every action" $gatewayKey $index $path $ssh._index) -}}
{{- end -}}
{{- if ne (toString (get $route "method")) "POST" -}}
{{- fail (printf "%s.routes[%d].method must be POST for effects-ssh (got %q): the service refuses every other method" $gatewayKey $index (toString (get $route "method"))) -}}
{{- end -}}
{{- if ne (toString (get $route "idempotency")) "key" -}}
{{- fail (printf "%s.routes[%d].idempotency must be key for effects-ssh: the journal is keyed by it, and the service refuses a request without one" $gatewayKey $index) -}}
{{- end -}}
{{- end -}}

{{- /* The journal. */ -}}
{{- $journal := $ssh.journal -}}
{{- $id := toString (default "" $journal.id) -}}
{{- if and $id (not (regexMatch `^[0-9a-f]{32}$` $id)) -}}
{{- fail (printf "%s.journal.id must be the 32 lower-case hex digits `journal init` printed (got %q), or empty to render the journal-init Job" $key $id) -}}
{{- end -}}
{{- $_ := set $plan "journalID" $id -}}
{{- if eq (toString $journal.storage) "persistentVolumeClaim" -}}
{{- include "shoal.requireShape" (dict "name" (printf "%s.journal.size" $key) "value" $journal.size "what" "a Kubernetes quantity such as 1Gi" "pattern" `^([0-9]+(\.[0-9]*)?|\.[0-9]+)(Ki|Mi|Gi|Ti|Pi|Ei|[kMGTPE])?$` "why" "it is the claim's storage request") -}}
{{- if not $journal.size -}}
{{- fail (printf "%s.journal.size is required with storage: persistentVolumeClaim" $key) -}}
{{- end -}}
{{- $_ := set $plan "claim" (printf "%s-journal" $name) -}}
{{- else if eq (toString $journal.storage) "existingClaim" -}}
{{- if not $journal.existingClaim -}}
{{- fail (printf "%s.journal.existingClaim is required with storage: existingClaim" $key) -}}
{{- end -}}
{{- $_ := set $plan "claim" (toString $journal.existingClaim) -}}
{{- else -}}
{{- fail (printf "%s.journal.storage must be persistentVolumeClaim or existingClaim (got %q): there is no emptyDir, because a journal deleted with the pod forgets every key, and a command whose outcome was unknown would run again" $key (toString $journal.storage)) -}}
{{- end -}}
{{- $maxEntries := include "shoal.effectsGatewayWhole" (dict "name" (printf "%s.journal.maxEntries" $key) "value" $journal.maxEntries) -}}
{{- if or (not $maxEntries) (lt (int64 $maxEntries) 1) (gt (int64 $maxEntries) 1048576) -}}
{{- fail (printf "%s.journal.maxEntries must be 1 to 1048576 (got %v)" $key $journal.maxEntries) -}}
{{- end -}}
{{- /* MaxRecordBytes and MaxCompactionBytes in service/journal.go. */ -}}
{{- $record := add (mul 2 (mul 4 (div (add $largestOutput 2) 3))) 2048 -}}
{{- if gt (mul (int64 $maxEntries) $record) 536870912 -}}
{{- fail (printf "%s.journal.maxEntries %v with a largest maxOutputBytes of %d allows %d bytes of live records, above the 536870912 a compaction may copy while requests wait; the binary refuses it. Lower one of them" $key $maxEntries $largestOutput (mul (int64 $maxEntries) $record)) -}}
{{- end -}}
{{- $_ := set $plan "maxEntries" (int64 $maxEntries) -}}

{{- /* Retention: journal retention ≥ the gateway's idempotency retention +
       clock skew. The gateway sends a key for up to its retention after the
       action's creation, on the explorer's clock; a journal that forgets the
       key sooner runs its command again. The gateway cannot read this
       service's retention, so the chart is where the two meet. */ -}}
{{- $gatewayRetention := int64 (include "shoal.durationMillis" (dict "name" (printf "%s.target.idempotencyRetention" $gatewayKey) "value" (default "" $target.idempotencyRetention))) -}}
{{- $skew := int64 (include "shoal.durationMillis" (dict "name" (printf "%s.journal.clockSkew" $key) "value" (default "" $journal.clockSkew))) -}}
{{- if le $skew 0 -}}
{{- fail (printf "%s.journal.clockSkew must be positive (got %q): the explorer's clock and this pod's are not the same clock" $key (toString $journal.clockSkew)) -}}
{{- end -}}
{{- $retention := add $gatewayRetention $skew -}}
{{- if $journal.retention -}}
{{- $override := int64 (include "shoal.durationMillis" (dict "name" (printf "%s.journal.retention" $key) "value" $journal.retention)) -}}
{{- if lt $override $retention -}}
{{- fail (printf "%s.journal.retention (%q) is below %s.target.idempotencyRetention (%q) + journal.clockSkew (%q): the gateway still sends a key the journal would have forgotten, and its command runs a second time. Leave it empty and the chart sets the minimum" $key (toString $journal.retention) $gatewayKey (toString $target.idempotencyRetention) (toString $journal.clockSkew)) -}}
{{- end -}}
{{- $retention = $override -}}
{{- end -}}
{{- if lt $retention 60000 -}}
{{- fail (printf "%s.journal.retention must be at least 1m" $key) -}}
{{- end -}}
{{- $_ := set $plan "retention" (printf "%dms" $retention) -}}

{{- /* The run bounds against the gateway. */ -}}
{{- $attach := int64 (include "shoal.durationMillis" (dict "name" (printf "%s.attachWait" $key) "value" $ssh.attachWait)) -}}
{{- if or (le $attach 0) (ge $attach $operation) -}}
{{- fail (printf "%s.attachWait (%q) must be positive and below %s.timing.operationTimeout (%q): a resend waits that long for the running command, inside one request" $key (toString $ssh.attachWait) $gatewayKey (toString $gateway.timing.operationTimeout)) -}}
{{- end -}}
{{- $_ := set $plan "attachWait" (printf "%dms" $attach) -}}
{{- $concurrent := include "shoal.effectsGatewayWhole" (dict "name" (printf "%s.maxConcurrent" $key) "value" $ssh.maxConcurrent) -}}
{{- if or (not $concurrent) (lt (int64 $concurrent) 1) (gt (int64 $concurrent) 64) -}}
{{- fail (printf "%s.maxConcurrent must be 1 to 64 (got %v)" $key $ssh.maxConcurrent) -}}
{{- end -}}
{{- $_ := set $plan "maxConcurrent" (int64 $concurrent) -}}

{{- /* Shutdown: runs in flight may finish (the longest command's whole
       budget), and the kubelet waits longer still. A shorter grace SIGKILLs
       a run, which is then journaled outcome_unknown: safe, never repeated,
       but an answer the operator has to chase. */ -}}
{{- $shutdown := $longest -}}
{{- if $ssh.shutdownGrace -}}
{{- $override := int64 (include "shoal.durationMillis" (dict "name" (printf "%s.shutdownGrace" $key) "value" $ssh.shutdownGrace)) -}}
{{- if lt $override $longest -}}
{{- fail (printf "%s.shutdownGrace (%q) is below %dms, the longest command's 2 × connectTimeout + runTimeout + 10s: a rollout would abandon that command mid-run. Leave it empty and the chart sets the minimum" $key (toString $ssh.shutdownGrace) $longest) -}}
{{- end -}}
{{- $shutdown = $override -}}
{{- end -}}
{{- $_ := set $plan "shutdownGrace" (printf "%dms" $shutdown) -}}
{{- $graceMinimum := add (div (add $shutdown 999) 1000) 10 -}}
{{- $grace := $graceMinimum -}}
{{- $graceOverride := include "shoal.effectsGatewayWhole" (dict "name" (printf "%s.terminationGracePeriodSeconds" $key) "value" $ssh.terminationGracePeriodSeconds) -}}
{{- if $graceOverride -}}
{{- if lt (int64 $graceOverride) $graceMinimum -}}
{{- fail (printf "%s.terminationGracePeriodSeconds (%v) is below %d, shutdownGrace + 10s: the kubelet would SIGKILL the process while it drains" $key $graceOverride $graceMinimum) -}}
{{- end -}}
{{- $grace = int64 $graceOverride -}}
{{- end -}}
{{- $_ := set $plan "grace" $grace -}}
{{- toJson $plan -}}
{{- end -}}

{{- /* Install notes for the effects-ssh entries; NOTES.txt includes this. */ -}}
{{- define "shoal.effectsSshNotes" -}}
{{- range $ssh := fromJsonArray (include "shoal.effectsSsh" .) }}
{{- if $ssh.enabled }}
{{- $plan := fromJson (include "shoal.effectsSshPlan" (dict "root" $ "ssh" $ssh)) }}
{{- if not $plan.journalID }}

  ******************************************************************************
  effects-ssh {{ $ssh.name }} ({{ $plan.name }}) is NOT serving yet: journal.id is
  empty, so this release rendered the journal-init Job and no Deployment.

  1. Read the journal's identity, which the Job printed:
       kubectl logs -n {{ $.Release.Namespace }} job/{{ $plan.name }}-init
  2. Set effectsSsh[{{ $ssh._index }}].journal.id to it and upgrade.

  The Job refuses a claim that already holds a journal: if it failed, read
  `shoal-effects-ssh journal list` on the claim before doing anything else.
  Never clear journal.id on a running entry; the chart refuses it.
  ******************************************************************************
{{- else }}
effects-ssh {{ $ssh.name }} ({{ $plan.name }}): serving {{ $plan.baseURL }} for effects gateway {{ $ssh.gateway }}, journal {{ $plan.journalID }} on claim {{ $plan.claim }}, -journal-retention {{ $plan.retention }}, terminationGracePeriodSeconds {{ $plan.grace }}, replicas {{ $ssh.replicas }}, Recreate.
{{- end }}
{{- if eq (toString $ssh.journal.storage) "persistentVolumeClaim" }}
  The journal's claim {{ $plan.claim }} is kept on uninstall
  (helm.sh/resource-policy: keep). It is the record of which commands ran:
  delete it only once no gateway can send its keys again.
{{- end }}
{{- end }}
{{- end -}}
{{- end -}}
