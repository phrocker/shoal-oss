package model

import (
	"net/http"
	"strings"
)

func configuredCacheIdentity(value any) (string, bool, error) {
	provider, ok := value.(CacheIdentityProvider)
	if !ok {
		return "", false, nil
	}
	identity, err := provider.CacheIdentity()
	if err != nil {
		return "", false, err
	}
	return identity, strings.TrimSpace(identity) != "", nil
}

func httpClientCacheIdentity(client *http.Client) (string, bool, error) {
	if client == nil {
		return "default-http-client-v1", true, nil
	}
	if identity, ok, err := configuredCacheIdentity(client); ok || err != nil {
		return identity, ok, err
	}
	if client.Transport == nil {
		return "custom-http-client-default-transport-v1", true, nil
	}
	if identity, ok, err := configuredCacheIdentity(client.Transport); ok || err != nil {
		return "custom-http-client-transport:" + identity, ok, err
	}
	return "", false, nil
}
