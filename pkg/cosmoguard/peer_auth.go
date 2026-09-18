package cosmoguard

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	peerTimestampHeader = "X-Cosmoguard-Peer-Timestamp"
	peerSignatureHeader = "X-Cosmoguard-Peer-Signature"
	peerKeyContext      = "cosmoguard/peer-api/key/v1"
	peerRequestProtocol = "cosmoguard/peer-api/request/v1"
	peerRequestMaxSkew  = 30 * time.Second
)

var errPeerAPIKeyUnavailable = errors.New("peer API key unavailable")

func derivePeerAPIKey(clusterKey []byte) []byte {
	mac := hmac.New(sha256.New, clusterKey)
	_, _ = mac.Write([]byte(peerKeyContext))
	return mac.Sum(nil)
}

func signPeerRequest(req *http.Request, key []byte, now time.Time) error {
	if len(key) == 0 {
		return errPeerAPIKeyUnavailable
	}
	if !validPeerRequestShape(req) {
		return errors.New("peer API request must be a bodyless GET")
	}
	timestamp := strconv.FormatInt(now.Unix(), 10)
	signature := peerRequestSignature(req, key, timestamp)
	req.Header.Set(peerTimestampHeader, timestamp)
	req.Header.Set(peerSignatureHeader, hex.EncodeToString(signature))
	return nil
}

func authenticatePeerRequest(req *http.Request, key []byte, now time.Time) error {
	if len(key) == 0 {
		return errPeerAPIKeyUnavailable
	}
	if !validPeerRequestShape(req) {
		return errors.New("invalid peer API request")
	}

	timestamps := req.Header.Values(peerTimestampHeader)
	signatures := req.Header.Values(peerSignatureHeader)
	if len(timestamps) != 1 || len(signatures) != 1 {
		return errors.New("invalid peer API authentication")
	}

	timestamp, err := strconv.ParseInt(timestamps[0], 10, 64)
	if err != nil || strconv.FormatInt(timestamp, 10) != timestamps[0] {
		return errors.New("invalid peer API authentication")
	}
	if timestamp < now.Add(-peerRequestMaxSkew).Unix() || timestamp > now.Add(peerRequestMaxSkew).Unix() {
		return errors.New("invalid peer API authentication")
	}

	provided, err := hex.DecodeString(signatures[0])
	if err != nil || len(provided) != sha256.Size || hex.EncodeToString(provided) != signatures[0] {
		return errors.New("invalid peer API authentication")
	}
	expected := peerRequestSignature(req, key, timestamps[0])
	if subtle.ConstantTimeCompare(provided, expected) != 1 {
		return errors.New("invalid peer API authentication")
	}
	return nil
}

func validPeerRequestShape(req *http.Request) bool {
	if req == nil || req.URL == nil || req.Method != http.MethodGet {
		return false
	}
	if req.ContentLength != 0 || len(req.TransferEncoding) != 0 {
		return false
	}
	return req.Body == nil || req.Body == http.NoBody
}

func peerRequestSignature(req *http.Request, key []byte, timestamp string) []byte {
	authority := req.Host
	if authority == "" {
		authority = req.URL.Host
	}
	message := strings.Join([]string{
		peerRequestProtocol,
		timestamp,
		req.Method,
		authority,
		req.URL.RequestURI(),
	}, "\n")
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(message))
	return mac.Sum(nil)
}

func peerAuthGate(key []byte) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if err := authenticatePeerRequest(req, key, time.Now()); err != nil {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, req)
		})
	}
}

func requirePeerAPIKey(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("fan-out: %w", errPeerAPIKeyUnavailable)
	}
	return nil
}
