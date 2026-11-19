// Path: server/mobile/torrserverkit/runtime_inspect.go
package torrserverkit

import (
    "context"
    "encoding/json"
    "errors"
    "path"
    "strconv"
    "strings"
    "time"

    "github.com/anacrolix/torrent"
    "github.com/anacrolix/torrent/storage/disabled"
)

const maximumInspectionDuration = 12 * time.Second

type inspectionConfig struct {
    RequestID string `json:"requestId"`
    CandidateID string `json:"candidateId"`
    InfoHashV1 string `json:"infoHashV1"`
    InfoHashV2 string `json:"infoHashV2"`
    Trackers []string `json:"trackers"`
    WebSeeds []string `json:"webSeeds"`
    TimeoutMs int64 `json:"timeoutMs"`
}

type inspectionFile struct {
    FileIndex int `json:"fileIndex"`
    Name string `json:"name"`
    RelativePath string `json:"relativePath"`
    LengthBytes string `json:"lengthBytes"`
}

type inspectionResult struct {
    CandidateID string `json:"candidateId"`
    Files []inspectionFile `json:"files"`
}

func validInspectionHash(value string, digits int) bool {
    if value == "" {
        return true
    }
    if len(value) != digits {
        return false
    }
    for _, c := range value {
        if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
            return false
        }
    }
    return true
}

func validateInspectionConfig(config inspectionConfig) error {
    if strings.TrimSpace(config.RequestID) == "" || len(config.RequestID) > 128 ||
        strings.TrimSpace(config.CandidateID) == "" ||
        (config.InfoHashV1 == "" && config.InfoHashV2 == "") ||
        !validInspectionHash(config.InfoHashV1, 40) ||
        !validInspectionHash(config.InfoHashV2, 64) {
        return errors.New("inspection_invalid_identity")
    }
    return nil
}

// Each inspection uses a separate client, no movie-data storage, and a strict
// network lifetime. A request can be cancelled without holding the runtime lock.
func (r *playbackRuntime) inspectTorrent(configJSON string) (string, error) {
    var config inspectionConfig
    if json.Unmarshal([]byte(configJSON), &config) != nil {
        return "", errors.New("inspection_invalid_request")
    }
    if err := validateInspectionConfig(config); err != nil {
        return "", err
    }

    duration := maximumInspectionDuration
    if config.TimeoutMs > 0 && time.Duration(config.TimeoutMs)*time.Millisecond < duration {
        duration = time.Duration(config.TimeoutMs) * time.Millisecond
    }
    ctx, cancel := context.WithTimeout(context.Background(), duration)
    defer cancel()

    r.mu.RLock()
    if r.closed {
        r.mu.RUnlock()
        return "", errors.New("inspection_runtime_closed")
    }
    r.inspectionMu.Lock()
    if r.inspections == nil {
        r.inspections = make(map[string]context.CancelFunc)
    }
    if _, exists := r.inspections[config.RequestID]; exists {
        r.inspectionMu.Unlock()
        r.mu.RUnlock()
        return "", errors.New("inspection_duplicate_request")
    }
    r.inspections[config.RequestID] = cancel
    r.inspectionMu.Unlock()
    r.mu.RUnlock()

    defer func() {
        r.inspectionMu.Lock()
        delete(r.inspections, config.RequestID)
        r.inspectionMu.Unlock()
    }()

    spec, _, err := candidateTorrentSpec(candidateConfig{
        CandidateID: config.CandidateID,
        InfoHashV1: config.InfoHashV1,
        InfoHashV2: config.InfoHashV2,
        Trackers: config.Trackers,
        WebSeeds: config.WebSeeds,
    })
    if err != nil {
        return "", errors.New("inspection_invalid_identity")
    }
    spec.DisallowDataDownload = true
    spec.DisallowDataUpload = true

    clientConfig := torrent.NewDefaultClientConfig()
    clientConfig.DefaultStorage = disabled.Client{}
    clientConfig.NoUpload = true
    clientConfig.NoDefaultPortForwarding = true
    clientConfig.DisableIPv6 = true
    clientConfig.DisableWebseeds = true
    clientConfig.ListenPort = 0

    client, err := torrent.NewClient(clientConfig)
    if err != nil {
        return "", errors.New("inspection_client_failed")
    }
    defer client.Close()

    t, _, err := client.AddTorrentSpec(spec)
    if err != nil {
        return "", errors.New("inspection_invalid_identity")
    }
    t.DisallowDataDownload()

    select {
    case <-t.GotInfo():
    case <-ctx.Done():
        if ctx.Err() == context.DeadlineExceeded {
            return "", errors.New("inspection_timeout")
        }
        return "", errors.New("inspection_cancelled")
    }

    info := t.Info()
    if info == nil || info.PieceLength <= 0 || (info.Private != nil && *info.Private) {
        if info != nil && info.Private != nil && *info.Private {
            return "", errors.New("inspection_private_torrent")
        }
        return "", errors.New("inspection_invalid_metainfo")
    }

    torrentFiles := t.Files()
    if len(torrentFiles) == 0 || len(torrentFiles) > 10000 {
        return "", errors.New("inspection_invalid_metainfo")
    }
    files := make([]inspectionFile, 0, len(torrentFiles))
    for index, file := range torrentFiles {
        relativePath := file.DisplayPath()
        if relativePath == "" || file.Length() < 0 ||
            strings.HasPrefix(relativePath, "/") ||
            strings.Contains("/"+relativePath+"/", "/../") {
            return "", errors.New("inspection_invalid_metainfo")
        }
        name := path.Base(relativePath)
        if name == "" || name == "." {
            return "", errors.New("inspection_invalid_metainfo")
        }
        files = append(files, inspectionFile{
            FileIndex: index,
            Name: name,
            RelativePath: relativePath,
            LengthBytes: strconv.FormatInt(file.Length(), 10),
        })
    }
    payload, err := json.Marshal(inspectionResult{
        CandidateID: config.CandidateID,
        Files: files,
    })
    if err != nil {
        return "", errors.New("inspection_invalid_metainfo")
    }
    return string(payload), nil
}

func (r *playbackRuntime) cancelInspection(requestID string) bool {
    r.inspectionMu.Lock()
    cancel := r.inspections[requestID]
    r.inspectionMu.Unlock()
    if cancel == nil {
        return false
    }
    cancel()
    return true
}

func (r *playbackRuntime) cancelInspections() {
    r.inspectionMu.Lock()
    cancels := make([]context.CancelFunc, 0, len(r.inspections))
    for _, cancel := range r.inspections {
        cancels = append(cancels, cancel)
    }
    r.inspectionMu.Unlock()
    for _, cancel := range cancels {
        cancel()
    }
}

