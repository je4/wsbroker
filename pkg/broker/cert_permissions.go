package broker

import (
	"crypto/x509"
	"strings"

	"github.com/je4/wsbrokerclient"
)

// CertPermissions holds the authorized client names and channel access permissions
// extracted from Subject Alternative Name (SAN) URIs in a client X.509 certificate.
type CertPermissions struct {
	HasClientRestrictions  bool
	HasChannelRestrictions bool
	Clients                map[string]struct{}
	Channels               map[string]wsbrokerclient.ConnectionMode
}

// ParseCertPermissions parses SAN URIs from an X.509 certificate and constructs CertPermissions.
// URIs follow the schemas:
// - wsbroker:client:<clientname>
// - wsbroker:channel:<channelname>:<ro|rw|wo>
func ParseCertPermissions(cert *x509.Certificate) *CertPermissions {
	cp := &CertPermissions{
		Clients:  make(map[string]struct{}),
		Channels: make(map[string]wsbrokerclient.ConnectionMode),
	}
	if cert == nil || len(cert.URIs) == 0 {
		return cp
	}

	for _, u := range cert.URIs {
		if u == nil {
			continue
		}
		if !strings.EqualFold(u.Scheme, "wsbroker") {
			continue
		}

		raw := strings.TrimPrefix(u.String(), u.Scheme+":")
		raw = strings.TrimPrefix(raw, "//")
		parts := strings.Split(raw, ":")
		if len(parts) == 0 {
			continue
		}

		targetType := strings.ToLower(parts[0])
		switch targetType {
		case "client":
			if len(parts) >= 2 {
				clientName := strings.Join(parts[1:], ":")
				if clientName != "" {
					cp.Clients[clientName] = struct{}{}
					cp.HasClientRestrictions = true
				}
			}
		case "channel":
			if len(parts) >= 3 {
				channelName := strings.Join(parts[1:len(parts)-1], ":")
				modeStr := parts[len(parts)-1]
				if mode, err := wsbrokerclient.ParseConnectionMode(modeStr); err == nil && channelName != "" {
					cp.Channels[channelName] = combineModes(cp.Channels[channelName], mode)
					cp.HasChannelRestrictions = true
				}
			}
		}
	}

	return cp
}

// combineModes merges two connection modes, choosing the most permissive mode.
func combineModes(existing, newMode wsbrokerclient.ConnectionMode) wsbrokerclient.ConnectionMode {
	if existing == "" {
		return newMode
	}
	if existing == wsbrokerclient.ModeRW || newMode == wsbrokerclient.ModeRW {
		return wsbrokerclient.ModeRW
	}
	if (existing == wsbrokerclient.ModeRO && newMode == wsbrokerclient.ModeWO) ||
		(existing == wsbrokerclient.ModeWO && newMode == wsbrokerclient.ModeRO) {
		return wsbrokerclient.ModeRW
	}
	if newMode != "" {
		return newMode
	}
	return existing
}

// HasRestrictions returns true if either client or channel restrictions were defined in the certificate.
func (cp *CertPermissions) HasRestrictions() bool {
	return cp != nil && (cp.HasClientRestrictions || cp.HasChannelRestrictions)
}

// CanAccessClient checks whether the certificate authorizes the specified client name.
func (cp *CertPermissions) CanAccessClient(clientName string) bool {
	if cp == nil || !cp.HasClientRestrictions || len(cp.Clients) == 0 {
		return true
	}
	if _, ok := cp.Clients["*"]; ok {
		return true
	}
	_, ok := cp.Clients[clientName]
	return ok
}

// CanAccessChannel checks whether the certificate authorizes access to the specified channel in requestedMode.
// Hierarchy:
// - ModeRW in certificate permits ModeRW, ModeRO, and ModeWO.
// - ModeRO in certificate permits ModeRO only.
// - ModeWO in certificate permits ModeWO only.
func (cp *CertPermissions) CanAccessChannel(channelName string, requestedMode wsbrokerclient.ConnectionMode) bool {
	if cp == nil || !cp.HasChannelRestrictions || len(cp.Channels) == 0 {
		return true
	}
	if granted, ok := cp.Channels[channelName]; ok {
		if IsModeAllowed(granted, requestedMode) {
			return true
		}
	}
	if granted, ok := cp.Channels["*"]; ok {
		if IsModeAllowed(granted, requestedMode) {
			return true
		}
	}
	return false
}

// IsModeAllowed checks whether grantedMode satisfies requestedMode.
// Hierarchy:
// - ModeRW grants ModeRW, ModeRO, and ModeWO.
// - ModeRO grants ModeRO only.
// - ModeWO grants ModeWO only.
func IsModeAllowed(grantedMode, requestedMode wsbrokerclient.ConnectionMode) bool {
	switch grantedMode {
	case wsbrokerclient.ModeRW:
		return requestedMode == wsbrokerclient.ModeRW || requestedMode == wsbrokerclient.ModeRO || requestedMode == wsbrokerclient.ModeWO
	case wsbrokerclient.ModeRO:
		return requestedMode == wsbrokerclient.ModeRO
	case wsbrokerclient.ModeWO:
		return requestedMode == wsbrokerclient.ModeWO
	default:
		return false
	}
}
