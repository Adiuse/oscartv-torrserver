package torrserverkit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	torrentstorage "github.com/anacrolix/torrent/storage"
)

const (
	sharedStoreMaximumBytes int64 = 512 << 20
	storeBlockSize          int64 = 1 << 20
	verifiedCopyChunkSize   int64 = 16 << 10
)

var errFullMovieVerificationEnvelope = errors.New(
	"piece-aligned verification envelope covers the entire target file",
)

type sessionStorage struct {
	mu sync.RWMutex

	session  *runtimePlaybackSession
	store    *blockStore
	bindings map[metainfo.Hash]*storageBinding
	capacity func() (int64, bool)
}

type storageBinding struct {
	mu sync.RWMutex

	session     *runtimePlaybackSession
	store       *blockStore
	candidateID string
	target      targetConfig

	privateAssetKey string
	sharedAssetKey  string

	info        *metainfo.Info
	targetStart int64
	targetEnd   int64

	torrent *torrent.Torrent

	completion           map[int]bool
	completionGeneration map[int]uint64
}

type runtimePiece struct {
	meta    metainfo.Piece
	binding *storageBinding
}

type blockStore struct {
	mu sync.Mutex

	root      string
	maxBytes  int64
	allocated int64
	sequence  uint64
	closed    bool

	assets           map[string]map[int64]*storeBlock
	demand           map[string]byteInterval
	currentDemand    map[string]byteInterval
	forwardDemand    map[string]byteInterval
	demandGeneration map[string]uint64
	recentDemand     map[string][]byteInterval
	notify           chan struct{}

	diskIO         atomic.Uint64
	diskReadBytes  atomic.Uint64
	diskWriteBytes atomic.Uint64

	onEvict func(
		asset string,
		start int64,
		end int64,
		generation uint64,
	)
	onAttributionEvict func(
		asset string,
		start int64,
		end int64,
		generation uint64,
	)
}

type storeBlock struct {
	path       string
	valid      []byteInterval
	lastAccess uint64
}

type byteInterval struct {
	start int64
	end   int64
}

func newSessionStorage(
	session *runtimePlaybackSession,
	store *blockStore,
) *sessionStorage {
	storage := &sessionStorage{
		session:  session,
		store:    store,
		bindings: make(map[metainfo.Hash]*storageBinding),
	}
	storage.capacity = func() (int64, bool) {
		return sharedStoreMaximumBytes, true
	}
	return storage
}

func (s *sessionStorage) register(
	hash metainfo.Hash,
	config candidateConfig,
) (*storageBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.bindings[hash]; exists {
		return nil, errors.New("duplicate torrent swarm registration")
	}

	binding := &storageBinding{
		session:         s.session,
		store:           s.store,
		candidateID:     config.CandidateID,
		target:          config.Target,
		privateAssetKey: "private:" + config.CandidateID,
		sharedAssetKey:  s.session.targetAssetKey,
		completion:           make(map[int]bool),
		completionGeneration: make(map[int]uint64),
	}
	s.bindings[hash] = binding
	return binding, nil
}

func (s *sessionStorage) unregister(
	hash metainfo.Hash,
) {
	s.mu.Lock()
	delete(s.bindings, hash)
	s.mu.Unlock()
}

func (s *sessionStorage) OpenTorrent(
	_ context.Context,
	info *metainfo.Info,
	infoHash metainfo.Hash,
) (torrentstorage.TorrentImpl, error) {
	s.mu.RLock()
	binding := s.bindings[infoHash]
	s.mu.RUnlock()

	if binding == nil {
		return torrentstorage.TorrentImpl{}, errors.New(
			"unregistered torrent storage",
		)
	}

	if err := binding.resolveTarget(info); err != nil {
		return torrentstorage.TorrentImpl{}, err
	}

	return torrentstorage.TorrentImpl{
		Piece: func(piece metainfo.Piece) torrentstorage.PieceImpl {
			return &runtimePiece{
				meta:    piece,
				binding: binding,
			}
		},
		Close: func() error {
			return nil
		},
		Capacity: &s.capacity,
	}, nil
}

func (b *storageBinding) resolveTarget(
	info *metainfo.Info,
) error {
	length, err := strconv.ParseInt(
		b.target.LengthBytes,
		10,
		64,
	)
	if err != nil || length <= 0 {
		return errors.New("invalid target length")
	}

	targetPath := normalizePath(
		b.target.RelativePath,
	)
	targetName := normalizePath(
		b.target.Name,
	)

	type resolved struct {
		start int64
		end   int64
		path  string
	}
	var pathMatches []resolved
	var nameMatches []resolved

	for file := range info.UpvertedFilesIter() {
		if file.Length != length {
			continue
		}

		path := normalizePath(
			file.DisplayPath(info),
		)
		item := resolved{
			start: file.TorrentOffset,
			end:   file.TorrentOffset + file.Length,
			path:  path,
		}

		if targetPath != "" &&
			(path == targetPath ||
				strings.HasSuffix(path, "/"+targetPath)) {
			pathMatches = append(pathMatches, item)
		}
		if basePath(path) == basePath(targetName) {
			nameMatches = append(nameMatches, item)
		}
	}

	var target resolved
	switch {
	case len(pathMatches) == 1:
		target = pathMatches[0]
	case len(pathMatches) > 1:
		return errors.New("torrent target path is ambiguous")
	case len(nameMatches) == 1:
		target = nameMatches[0]
	case len(nameMatches) > 1:
		return errors.New("torrent target name is ambiguous")
	default:
		return errors.New("torrent target file not found")
	}

	b.mu.Lock()
	b.info = info
	b.targetStart = target.start
	b.targetEnd = target.end
	b.mu.Unlock()
	return nil
}

func (b *storageBinding) attachTorrent(
	t *torrent.Torrent,
) {
	b.mu.Lock()
	b.torrent = t
	b.mu.Unlock()
}

func (p *runtimePiece) ReadAt(
	buffer []byte,
	offset int64,
) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}

	absoluteStart := p.meta.Offset() + offset
	return p.binding.readRange(
		absoluteStart,
		buffer,
	)
}

func (p *runtimePiece) WriteAt(
	buffer []byte,
	offset int64,
) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}

	absoluteStart := p.meta.Offset() + offset
	n, duplicate, err := p.binding.writeRange(
		absoluteStart,
		buffer,
	)
	if n > 0 {
		p.binding.session.recordWrite(
			p.binding.candidateID,
			int64(n),
			duplicate,
		)
	}
	return n, err
}

func (p *runtimePiece) MarkComplete() error {
	pieceStart := p.meta.Offset()
	pieceEnd := pieceStart + p.meta.Length()
	wasComplete := p.binding.isComplete(p.meta.Index())

	p.binding.store.mu.Lock()
	available := p.binding.pieceRangeAvailableLocked(
		pieceStart,
		pieceEnd,
	)
	revision := p.binding.store.sequence
	p.binding.setCompletion(
		p.meta.Index(),
		available,
		revision,
	)
	p.binding.store.mu.Unlock()

	if !available {
		p.binding.session.recordVerification(
			p.binding.candidateID,
			false,
			false,
		)
		return errors.New("verified torrent piece data was evicted before completion commit")
	}
	if !wasComplete {
		p.binding.mu.RLock()
		targetStart := p.binding.targetStart
		targetEnd := p.binding.targetEnd
		p.binding.mu.RUnlock()
		verifiedTarget := targetEnd > targetStart &&
			pieceStart < targetEnd &&
			pieceEnd > targetStart
		p.binding.session.recordVerification(
			p.binding.candidateID,
			true,
			verifiedTarget,
		)
	}
	return p.binding.publishVerifiedTarget(pieceStart, pieceEnd)
}

func (p *runtimePiece) MarkNotComplete() error {
	p.binding.session.recordVerification(p.binding.candidateID, false, false)
	generation := p.binding.store.currentRevision()
	p.binding.setCompletion(p.meta.Index(), false, generation)
	return p.binding.store.invalidate(
		p.binding.privateAssetKey,
		p.meta.Offset(),
		p.meta.Offset()+p.meta.Length(),
	)
}

func (p *runtimePiece) Completion() torrentstorage.Completion {
	return torrentstorage.Completion{
		Ok: true,
		Complete: p.binding.isComplete(p.meta.Index()),
	}
}

func (b *storageBinding) readRange(
	absoluteStart int64,
	buffer []byte,
) (int, error) {
	absoluteEnd := absoluteStart + int64(len(buffer))
	cursor := absoluteStart
	written := 0

	b.mu.RLock()
	targetStart := b.targetStart
	targetEnd := b.targetEnd
	b.mu.RUnlock()

	for cursor < absoluteEnd {
		segmentEnd := absoluteEnd
		target := cursor >= targetStart && cursor < targetEnd

		if cursor < targetStart && segmentEnd > targetStart {
			segmentEnd = targetStart
		}
		if target && segmentEnd > targetEnd {
			segmentEnd = targetEnd
		}

		length := int(segmentEnd - cursor)
		destination := buffer[written : written+length]

		var err error
		if target {
			err = b.store.read(
				b.privateAssetKey,
				cursor,
				destination,
			)
			if err != nil {
				err = b.store.read(
					b.sharedAssetKey,
					cursor-targetStart,
					destination,
				)
			}
		} else {
			err = b.store.read(
				b.privateAssetKey,
				cursor,
				destination,
			)
		}

		if err != nil {
			if written > 0 {
				return written, io.EOF
			}
			return 0, io.EOF
		}

		written += length
		cursor = segmentEnd
	}

	return written, nil
}

func (b *storageBinding) writeRange(
	absoluteStart int64,
	buffer []byte,
) (int, int64, error) {
	absoluteEnd := absoluteStart + int64(len(buffer))
	cursor := absoluteStart
	written := 0
	var duplicate int64

	b.mu.RLock()
	targetStart := b.targetStart
	targetEnd := b.targetEnd
	b.mu.RUnlock()

	for cursor < absoluteEnd {
		segmentEnd := absoluteEnd
		target := cursor >= targetStart && cursor < targetEnd

		if cursor < targetStart && segmentEnd > targetStart {
			segmentEnd = targetStart
		}
		if target && segmentEnd > targetEnd {
			segmentEnd = targetEnd
		}

		length := int(segmentEnd - cursor)
		source := buffer[written : written+length]

		privateDuplicate, err := b.store.write(
			b.privateAssetKey,
			cursor,
			source,
			true,
		)
		if err != nil {
			return written, duplicate, err
		}

		segmentDuplicate := privateDuplicate
		if target {
			sharedDuplicate := b.store.coveredLength(
				b.sharedAssetKey,
				cursor-targetStart,
				segmentEnd-targetStart,
			)
			if sharedDuplicate > segmentDuplicate {
				segmentDuplicate = sharedDuplicate
			}
		}

		duplicate += segmentDuplicate
		written += length
		cursor = segmentEnd
	}

	return written, duplicate, nil
}

func (b *storageBinding) publishVerifiedTarget(
    pieceStart int64,
    pieceEnd int64,
) error {
    b.mu.RLock()
    targetStart := b.targetStart
    targetEnd := b.targetEnd
    b.mu.RUnlock()

    sharedDemand, hasDemand :=
        b.store.demandRange(
            b.sharedAssetKey,
        )
    if !hasDemand {
        return nil
    }

    start := maxInt64(
        maxInt64(pieceStart, targetStart),
        targetStart+sharedDemand.start,
    )
    end := minInt64(
        minInt64(pieceEnd, targetEnd),
        targetStart+sharedDemand.end,
    )
    if end <= start {
        return nil
    }

    var newlyVerified []byteInterval
    notifySharedVerified := func() {
        for _, interval := range newlyVerified {
            b.session.onSharedVerified(
                b.candidateID,
                interval.start,
                interval.end,
            )
        }
    }

    for cursor := start; cursor < end; {
        segmentEnd := cursor + verifiedCopyChunkSize
        if segmentEnd > end {
            segmentEnd = end
        }

        buffer := make(
            []byte,
            int(segmentEnd-cursor),
        )

        n, err := b.readRange(
            cursor,
            buffer,
        )
        if err != nil || n != len(buffer) {
            cursor = segmentEnd
            continue
        }

        added, writeErr := b.store.writeVerified(
            b.sharedAssetKey,
            cursor-targetStart,
            buffer,
            func(revision uint64, committed []byteInterval) {
                b.session.recordOwnership(
                    b.candidateID,
                    revision,
                    committed,
                )
            },
        )
        newlyVerified = append(
            newlyVerified,
            added...,
        )
        if writeErr != nil {
            notifySharedVerified()
            return writeErr
        }

        cursor = segmentEnd
    }

    notifySharedVerified()
    return nil
}

func (b *storageBinding) setCompletion(
	index int,
	complete bool,
	generation uint64,
) {
	b.mu.Lock()
	if generation >= b.completionGeneration[index] {
		b.completion[index] = complete
		b.completionGeneration[index] = generation
	}
	b.mu.Unlock()
}

func (b *storageBinding) isComplete(index int) bool {
	b.mu.RLock()
	complete := b.completion[index]
	b.mu.RUnlock()
	return complete
}

func (b *storageBinding) pieceRangeAvailableLocked(
	start int64,
	end int64,
) bool {
	if end <= start {
		return false
	}
	b.mu.RLock()
	targetStart := b.targetStart
	targetEnd := b.targetEnd
	privateAssetKey := b.privateAssetKey
	sharedAssetKey := b.sharedAssetKey
	b.mu.RUnlock()
	if targetEnd <= targetStart {
		return false
	}

	privateAvailable := func(rangeStart, rangeEnd int64) bool {
		return coversRange(
			b.store.availableIntervalsLocked(privateAssetKey, rangeStart, rangeEnd),
			rangeStart,
			rangeEnd,
		)
	}
	if start < targetStart {
		beforeEnd := minInt64(end, targetStart)
		if !privateAvailable(start, beforeEnd) {
			return false
		}
	}
	targetSegmentStart := maxInt64(start, targetStart)
	targetSegmentEnd := minInt64(end, targetEnd)
	if targetSegmentEnd > targetSegmentStart {
		privateHasTarget := privateAvailable(targetSegmentStart, targetSegmentEnd)
		sharedAvailable := b.store.availableIntervalsLocked(
			sharedAssetKey,
			targetSegmentStart-targetStart,
			targetSegmentEnd-targetStart,
		)
		sharedHasTarget := coversRange(
			sharedAvailable,
			targetSegmentStart-targetStart,
			targetSegmentEnd-targetStart,
		)
		if !privateHasTarget && !sharedHasTarget {
			return false
		}
	}
	if end > targetEnd {
		afterStart := maxInt64(start, targetEnd)
		if !privateAvailable(afterStart, end) {
			return false
		}
	}
	return true
}

func (s *blockStore) availableIntervalsLocked(
	asset string,
	start int64,
	end int64,
) []byteInterval {
	if end <= start {
		return nil
	}
	blocks := s.assets[asset]
	first := start / storeBlockSize
	last := (end - 1) / storeBlockSize
	var available []byteInterval
	for index := first; index <= last; index++ {
		block := blocks[index]
		if block == nil {
			continue
		}
		blockStart := index * storeBlockSize
		for _, valid := range block.valid {
			rangeStart := maxInt64(start, blockStart+valid.start)
			rangeEnd := minInt64(end, blockStart+valid.end)
			if rangeEnd > rangeStart {
				available = addInterval(available, byteInterval{
					start: rangeStart,
					end: rangeEnd,
				})
			}
		}
	}
	return available
}

func (b *storageBinding) demandWindow(
	offset int64,
	length int64,
) (byteInterval, error) {
	b.mu.RLock()
	targetStart := b.targetStart
	targetEnd := b.targetEnd
	info := b.info
	b.mu.RUnlock()

	fileLength := targetEnd - targetStart
	start := maxInt64(offset, 0)
	if fileLength <= 0 || start >= fileLength || info == nil || info.PieceLength <= 0 {
		return byteInterval{}, nil
	}
	remaining := fileLength - start
	end := start + minInt64(maxInt64(length, 0), remaining)
	if end <= start {
		return byteInterval{}, nil
	}

	absoluteStart := targetStart + start
	absoluteEnd := targetStart + end
	pieceLength := info.PieceLength
	pieceStart := (absoluteStart / pieceLength) * pieceLength
	pieceEnd := absoluteEnd
	if remainder := absoluteEnd % pieceLength; remainder != 0 {
		padding := pieceLength - remainder
		maxValue := int64(^uint64(0) >> 1)
		if absoluteEnd > maxValue-padding {
			return byteInterval{}, errors.New("piece-aligned verification envelope overflow")
		}
		pieceEnd += padding
	}
	if totalLength := info.TotalLength(); totalLength > 0 {
		pieceEnd = minInt64(pieceEnd, totalLength)
	}
	if pieceStart <= targetStart && pieceEnd >= targetEnd {
		return byteInterval{}, errFullMovieVerificationEnvelope
	}
	return byteInterval{start: pieceStart, end: pieceEnd}, nil
}

func (b *storageBinding) setDemand(
	offset int64,
	currentLength int64,
	totalLength int64,
	generation uint64,
) error {
	currentWindow, err := b.demandWindow(offset, currentLength)
	if err != nil {
		return err
	}
	totalWindow, err := b.demandWindow(offset, totalLength)
	if err != nil {
		return err
	}
	if totalWindow.end <= totalWindow.start {
		return b.store.clearDemandForGeneration(
			b.privateAssetKey,
			generation,
		)
	}
	currentEnd := totalWindow.start
	if currentWindow.end > currentWindow.start {
		currentEnd = minInt64(currentWindow.end, totalWindow.end)
	}
	return b.store.setDemandRangesForGeneration(
		b.privateAssetKey,
		totalWindow.start,
		currentEnd,
		totalWindow.end,
		generation,
	)
}

func (b *storageBinding) clearDemand(
	generation uint64,
) error {
	return b.store.clearDemandForGeneration(
		b.privateAssetKey,
		generation,
	)
}

func (b *storageBinding) verifySharedRange(
	sharedStart int64,
	sharedEnd int64,
) {
	b.mu.RLock()
	t := b.torrent
	info := b.info
	targetStart := b.targetStart
	b.mu.RUnlock()
	if t == nil || info == nil {
		return
	}
	absoluteStart := targetStart + sharedStart
	absoluteEnd := targetStart + sharedEnd
	pieceLength := info.PieceLength
	if pieceLength <= 0 {
		return
	}
	first := int(absoluteStart / pieceLength)
	last := int((absoluteEnd - 1) / pieceLength)
	for index := first; index <= last; index++ {
		if b.isComplete(index) {
			continue
		}
		go func(pieceIndex int) {
			_ = t.Piece(pieceIndex).VerifyData()
		}(index)
	}
}

func (b *storageBinding) invalidateSharedRange(
	sharedStart int64,
	sharedEnd int64,
	generation uint64,
) {
	b.mu.RLock()
	targetStart := b.targetStart
	b.mu.RUnlock()
	b.invalidateCompletionRange(targetStart+sharedStart, targetStart+sharedEnd, generation)
}

func (b *storageBinding) invalidatePrivateRange(
	start int64,
	end int64,
	generation uint64,
) {
	b.invalidateCompletionRange(start, end, generation)
}

func (b *storageBinding) invalidateCompletionRange(
	start int64,
	end int64,
	generation uint64,
) {
	if end <= start {
		return
	}
	b.mu.RLock()
	info := b.info
	t := b.torrent
	b.mu.RUnlock()
	if info == nil || info.PieceLength <= 0 {
		return
	}
	first := int(start / info.PieceLength)
	last := int((end - 1) / info.PieceLength)
	for index := first; index <= last; index++ {
		b.setCompletion(index, false, generation)
		if t != nil && !b.isComplete(index) {
			t.Piece(index).UpdateCompletion()
		}
	}
}

func newBlockStore(
	root string,
	maxBytes int64,
) (*blockStore, error) {
	if maxBytes <= 0 || maxBytes > sharedStoreMaximumBytes {
		return nil, errors.New("invalid shared store capacity")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &blockStore{
		root:             root,
		maxBytes:         maxBytes,
		assets:           make(map[string]map[int64]*storeBlock),
		demand:           make(map[string]byteInterval),
		currentDemand:    make(map[string]byteInterval),
		forwardDemand:    make(map[string]byteInterval),
		demandGeneration: make(map[string]uint64),
		recentDemand:     make(map[string][]byteInterval),
		notify:           make(chan struct{}),
	}, nil
}

func (s *blockStore) setDemand(
	asset string,
	start int64,
	end int64,
) error {
	s.mu.Lock()
	generation := s.demandGeneration[asset] + 1
	s.mu.Unlock()
	return s.setDemandForGeneration(asset, start, end, generation)
}

func (s *blockStore) setDemandForGeneration(
	asset string,
	start int64,
	end int64,
	generation uint64,
) error {
	return s.setDemandRangesForGeneration(
		asset,
		start,
		end,
		end,
		generation,
	)
}

func (s *blockStore) setDemandRangesForGeneration(
	asset string,
	start int64,
	currentEnd int64,
	end int64,
	generation uint64,
) error {
	if start < 0 {
		start = 0
	}
	if currentEnd < start {
		currentEnd = start
	}
	if end < currentEnd {
		end = currentEnd
	}
	if end <= start {
		return s.clearDemandForGeneration(asset, generation)
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("torrent playback storage is closed")
	}
	if generation < s.demandGeneration[asset] {
		s.mu.Unlock()
		return nil
	}
	if previous, ok := s.demand[asset]; ok &&
		(previous.start != start || previous.end != end) &&
		strings.HasPrefix(asset, "shared:") {
		recent := append(s.recentDemand[asset], previous)
		if len(recent) > 2 {
			recent = append([]byteInterval(nil), recent[len(recent)-2:]...)
		}
		s.recentDemand[asset] = recent
	}
	s.demandGeneration[asset] = generation
	s.demand[asset] = byteInterval{start: start, end: end}
	if currentEnd > start {
		s.currentDemand[asset] = byteInterval{start: start, end: currentEnd}
	} else {
		delete(s.currentDemand, asset)
	}
	if end > currentEnd {
		s.forwardDemand[asset] = byteInterval{start: currentEnd, end: end}
	} else {
		delete(s.forwardDemand, asset)
	}
	evicted, err := s.pruneOutsideDemandLocked(asset)
	s.signalLocked()
	revision := s.sequence
	s.mu.Unlock()
	s.notifyEvictions(asset, evicted, revision)
	return err
}

func (s *blockStore) clearDemand(asset string) error {
	s.mu.Lock()
	generation := s.demandGeneration[asset] + 1
	s.mu.Unlock()
	return s.clearDemandForGeneration(asset, generation)
}

func (s *blockStore) clearDemandForGeneration(
	asset string,
	generation uint64,
) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("torrent playback storage is closed")
	}
	if generation < s.demandGeneration[asset] {
		s.mu.Unlock()
		return nil
	}
	if previous, ok := s.demand[asset]; ok && strings.HasPrefix(asset, "shared:") {
		recent := append(s.recentDemand[asset], previous)
		if len(recent) > 2 {
			recent = append([]byteInterval(nil), recent[len(recent)-2:]...)
		}
		s.recentDemand[asset] = recent
	}
	s.demandGeneration[asset] = generation
	delete(s.demand, asset)
	delete(s.currentDemand, asset)
	delete(s.forwardDemand, asset)
	evicted, err := s.pruneOutsideDemandLocked(asset)
	s.signalLocked()
    revision := s.sequence
    s.mu.Unlock()
    s.notifyEvictions(asset, evicted, revision)
	return err
}

func (s *blockStore) currentRevision() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sequence
}

func (s *blockStore) demandRange(asset string) (byteInterval, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	demand, ok := s.demand[asset]
	return demand, ok
}

func (s *blockStore) allowedRangesLocked(asset string) []byteInterval {
	var allowed []byteInterval
	if demand, ok := s.demand[asset]; ok {
		allowed = addInterval(allowed, demand)
	}
	for _, recent := range s.recentDemand[asset] {
		allowed = addInterval(allowed, recent)
	}
	return allowed
}

func (s *blockStore) pruneOutsideDemandLocked(
	asset string,
) ([]byteInterval, error) {
	blocks := s.assets[asset]
	if len(blocks) == 0 {
		return nil, nil
	}
	allowed := s.allowedRangesLocked(asset)
	var evicted []byteInterval

	for index, block := range blocks {
		blockStart := index * storeBlockSize
		blockEnd := blockStart + storeBlockSize
		var keep []byteInterval
		for _, interval := range allowed {
			start := maxInt64(blockStart, interval.start)
			end := minInt64(blockEnd, interval.end)
			if end > start {
				keep = addInterval(keep, byteInterval{
					start: start - blockStart,
					end:   end - blockStart,
				})
			}
		}

		if len(block.valid) == 0 {
			if err := os.Remove(block.path); err != nil && !os.IsNotExist(err) {
				return evicted, err
			}
			delete(blocks, index)
			s.allocated -= storeBlockSize
			continue
		}

		retained := intersectIntervals(block.valid, keep)
		removed := subtractIntervals(block.valid, keep)
		if len(removed) == 0 {
			continue
		}
		s.sequence++

		previousValid := append([]byteInterval(nil), block.valid...)
		if len(retained) == 0 {
			if err := os.Remove(block.path); err != nil && !os.IsNotExist(err) {
				block.valid = nil
				for _, interval := range previousValid {
					evicted = append(evicted, byteInterval{
						start: blockStart + interval.start,
						end:   blockStart + interval.end,
					})
				}
				return evicted, fmt.Errorf("failed to prune torrent block: %w", err)
			}
			delete(blocks, index)
			s.allocated -= storeBlockSize
		} else if err := s.rewriteBlockLocked(block, retained); err != nil {
			removeErr := os.Remove(block.path)
			if removeErr != nil && !os.IsNotExist(removeErr) {
				block.valid = nil
				for _, interval := range previousValid {
					evicted = append(evicted, byteInterval{
						start: blockStart + interval.start,
						end:   blockStart + interval.end,
					})
				}
				return evicted, fmt.Errorf(
					"failed to rewrite torrent block: %v; failed to remove unreadable block: %w",
					err,
					removeErr,
				)
			}
			delete(blocks, index)
			s.allocated -= storeBlockSize
			for _, interval := range previousValid {
				evicted = append(evicted, byteInterval{
					start: blockStart + interval.start,
					end:   blockStart + interval.end,
				})
			}
			return evicted, fmt.Errorf("failed to retain torrent block during pruning: %w", err)
		}

		for _, interval := range removed {
			evicted = append(evicted, byteInterval{
				start: blockStart + interval.start,
				end:   blockStart + interval.end,
			})
		}
	}

	if len(blocks) == 0 {
		delete(s.assets, asset)
	}
	return evicted, nil
}

func intersectIntervals(values []byteInterval, allowed []byteInterval) []byteInterval {
	var result []byteInterval
	for _, value := range values {
		for _, keep := range allowed {
			start := maxInt64(value.start, keep.start)
			end := minInt64(value.end, keep.end)
			if end > start {
				result = addInterval(result, byteInterval{start: start, end: end})
			}
		}
	}
	return result
}

func subtractIntervals(values []byteInterval, allowed []byteInterval) []byteInterval {
	result := append([]byteInterval(nil), values...)
	for _, keep := range allowed {
		result = removeInterval(result, keep)
	}
	return result
}

func (s *blockStore) rewriteBlockLocked(
    block *storeBlock,
    retained []byteInterval,
) error {
    source, err := os.Open(block.path)
    if err != nil {
        return err
    }
    defer source.Close()

    temp, err := os.CreateTemp(
        filepath.Dir(block.path),
        ".block-prune-*.tmp",
    )
    if err != nil {
        return err
    }
    tempPath := temp.Name()
    defer os.Remove(tempPath)

    if err := temp.Chmod(0o600); err != nil {
        temp.Close()
        return err
    }
    if err := temp.Truncate(storeBlockSize); err != nil {
        temp.Close()
        return err
    }

    buffer := make([]byte, int(verifiedCopyChunkSize))
    for _, interval := range retained {
        if interval.start < 0 ||
            interval.end > storeBlockSize ||
            interval.end <= interval.start {
            temp.Close()
            return errors.New("invalid retained torrent storage range")
        }

        for cursor := interval.start; cursor < interval.end; {
            length := int(
                minInt64(
                    int64(len(buffer)),
                    interval.end-cursor,
                ),
            )
            n, readErr := source.ReadAt(
                buffer[:length],
                cursor,
            )
            if readErr != nil && readErr != io.EOF {
                temp.Close()
                return readErr
            }
            if n != length {
                temp.Close()
                return io.ErrUnexpectedEOF
            }

            written, writeErr := temp.WriteAt(
                buffer[:length],
                cursor,
            )
            if writeErr != nil {
                temp.Close()
                return writeErr
            }
            if written != length {
                temp.Close()
                return io.ErrShortWrite
            }
            cursor += int64(length)
        }
    }

    if err := temp.Close(); err != nil {
        return err
    }
    if err := os.Rename(tempPath, block.path); err != nil {
        return err
    }

    block.valid = append([]byteInterval(nil), retained...)
    return nil
}

func (s *blockStore) notifyEvictions(
	asset string,
	evicted []byteInterval,
	generation uint64,
) {
	for _, interval := range evicted {
		if s.onAttributionEvict != nil {
			s.onAttributionEvict(
				asset,
				interval.start,
				interval.end,
				generation,
			)
		}
		if s.onEvict != nil {
			go s.onEvict(
				asset,
				interval.start,
				interval.end,
				generation,
			)
		}
	}
}

func (s *blockStore) validateDemandLocked(
	asset string,
	start int64,
	end int64,
) error {
	demand, ok := s.demand[asset]
	if !ok ||
		start < demand.start ||
		end > demand.end {
		return errors.New(
			"torrent write is outside the active playback window",
		)
	}
	return nil
}

func (s *blockStore) write(
	asset string,
	offset int64,
	data []byte,
	overwrite bool,
) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.validateDemandLocked(
		asset,
		offset,
		offset+int64(len(data)),
	); err != nil {
		return 0, err
	}

	var duplicate int64
	cursor := offset
	consumed := 0

	for consumed < len(data) {
		blockIndex := cursor / storeBlockSize
		blockOffset := cursor % storeBlockSize
		length := int(
			minInt64(
				int64(len(data)-consumed),
				storeBlockSize-blockOffset,
			),
		)

		block, err := s.ensureBlockLocked(
			asset,
			blockIndex,
		)
		if err != nil {
			return duplicate, err
		}

		rangeStart := blockOffset
		rangeEnd := blockOffset + int64(length)
		duplicate += coveredLength(
			block.valid,
			rangeStart,
			rangeEnd,
		)

		if overwrite ||
			!coversRange(
				block.valid,
				rangeStart,
				rangeEnd,
			) {
			file, err := os.OpenFile(
				block.path,
				os.O_WRONLY,
				0o600,
			)
			if err != nil {
				return duplicate, err
			}

			n, writeErr := file.WriteAt(
				data[consumed:consumed+length],
				blockOffset,
			)
			closeErr := file.Close()
			if writeErr != nil {
				return duplicate, writeErr
			}
			if closeErr != nil {
				return duplicate, closeErr
			}
			if n != length {
				return duplicate, io.ErrShortWrite
			}

			s.diskIO.Add(uint64(n))
			s.diskWriteBytes.Add(uint64(n))
			block.valid = addInterval(
				block.valid,
				byteInterval{
					start: rangeStart,
					end:   rangeEnd,
				},
			)
		}

		s.sequence++
		block.lastAccess = s.sequence

		cursor += int64(length)
		consumed += length
	}

	s.signalLocked()
	return duplicate, nil
}

func (s *blockStore) writeVerified(
	asset string,
	offset int64,
	data []byte,
	onCommitted func(uint64, []byteInterval),
) ([]byteInterval, error) {
	if len(data) == 0 {
		return nil, nil
	}

	s.mu.Lock()

	if err := s.validateDemandLocked(
		asset,
		offset,
		offset+int64(len(data)),
	); err != nil {
		s.mu.Unlock()
		return nil, err
	}

	var added []byteInterval
	defer func() {
		s.signalLocked()
		s.mu.Unlock()
	}()

	cursor := offset
	consumed := 0

	for consumed < len(data) {
		blockIndex := cursor / storeBlockSize
		blockOffset := cursor % storeBlockSize
		length := int(
			minInt64(
				int64(len(data)-consumed),
				storeBlockSize-blockOffset,
			),
		)

		block, err := s.ensureBlockLocked(
			asset,
			blockIndex,
		)
		if err != nil {
			return added, err
		}

		rangeStart := blockOffset
		rangeEnd := blockOffset + int64(length)
		gaps := subtractCovered(
			byteInterval{
				start: rangeStart,
				end:   rangeEnd,
			},
			block.valid,
		)

		if len(gaps) > 0 {
			file, err := os.OpenFile(
				block.path,
				os.O_WRONLY,
				0o600,
			)
			if err != nil {
				return added, err
			}

			for _, gap := range gaps {
				dataStart :=
					consumed +
						int(gap.start-rangeStart)
				dataEnd :=
					consumed +
						int(gap.end-rangeStart)

				n, writeErr := file.WriteAt(
					data[dataStart:dataEnd],
					gap.start,
				)
				if writeErr != nil {
					file.Close()
					return added, writeErr
				}
				if n != dataEnd-dataStart {
					file.Close()
					return added, io.ErrShortWrite
				}

				s.diskIO.Add(uint64(n))
				s.diskWriteBytes.Add(uint64(n))
				s.sequence++
				block.lastAccess = s.sequence
				block.valid = addInterval(
					block.valid,
					gap,
				)
				committed := byteInterval{
					start: blockIndex*storeBlockSize + gap.start,
					end:   blockIndex*storeBlockSize + gap.end,
				}
				added = append(
					added,
					committed,
				)
				onCommitted(s.sequence, []byteInterval{committed})
			}

			if err := file.Close(); err != nil {
				return added, err
			}
		}

		s.sequence++
		block.lastAccess = s.sequence

		cursor += int64(length)
		consumed += length
	}

	return added, nil
}

func (s *blockStore) read(
	asset string,
	offset int64,
	buffer []byte,
) error {
	if len(buffer) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cursor := offset
	consumed := 0

	for consumed < len(buffer) {
		blockIndex := cursor / storeBlockSize
		blockOffset := cursor % storeBlockSize
		length := int(
			minInt64(
				int64(len(buffer)-consumed),
				storeBlockSize-blockOffset,
			),
		)

		block := s.assets[asset][blockIndex]
		if block == nil ||
			!coversRange(
				block.valid,
				blockOffset,
				blockOffset+int64(length),
			) {
			return io.EOF
		}

		file, err := os.Open(
			block.path,
		)
		if err != nil {
			return err
		}

		n, readErr := file.ReadAt(
			buffer[consumed:consumed+length],
			blockOffset,
		)
		closeErr := file.Close()

		if readErr != nil &&
			readErr != io.EOF {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if n != length {
			return io.EOF
		}

		s.diskIO.Add(uint64(n))
		s.diskReadBytes.Add(uint64(n))
		s.sequence++
		block.lastAccess = s.sequence

		cursor += int64(length)
		consumed += length
	}

	return nil
}

func (s *blockStore) waitRead(
	ctx context.Context,
	asset string,
	offset int64,
	buffer []byte,
) error {
	for {
		s.mu.Lock()
		notify := s.notify
		closed := s.closed
		s.mu.Unlock()

		if closed {
			return errors.New("torrent playback storage is closed")
		}

		err := s.read(
			asset,
			offset,
			buffer,
		)
		if err == nil {
			return nil
		}
		if !errors.Is(err, io.EOF) {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

func (s *blockStore) coveredLength(
	asset string,
	start int64,
	end int64,
) int64 {
	if end <= start {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var total int64
	cursor := start

	for cursor < end {
		blockIndex := cursor / storeBlockSize
		blockOffset := cursor % storeBlockSize
		segmentEnd := minInt64(
			end,
			(blockIndex+1)*storeBlockSize,
		)

		block := s.assets[asset][blockIndex]
		if block != nil {
			total += coveredLength(
				block.valid,
				blockOffset,
				blockOffset+
					(segmentEnd-cursor),
			)
		}
		cursor = segmentEnd
	}

	return total
}

func (s *blockStore) invalidate(
    asset string,
    start int64,
    end int64,
) error {
    if end <= start {
        return nil
    }

    var evicted []byteInterval
    s.mu.Lock()
	
    cursor := start

    for cursor < end {
        blockIndex := cursor / storeBlockSize
        blockOffset := cursor % storeBlockSize
        segmentEnd := minInt64(
            end,
            (blockIndex+1)*storeBlockSize,
        )

        blocks := s.assets[asset]
        block := blocks[blockIndex]
        if block != nil {
            rangeEnd := blockOffset + segmentEnd - cursor
            invalidRange := byteInterval{
                start: blockOffset,
                end: rangeEnd,
            }
            var removed []byteInterval
            for _, valid := range block.valid {
                removedStart := maxInt64(valid.start, invalidRange.start)
                removedEnd := minInt64(valid.end, invalidRange.end)
                if removedEnd > removedStart {
                    removed = append(
                        removed,
                        byteInterval{
                            start: removedStart,
                            end: removedEnd,
                        },
                    )
                }
            }

            if len(removed) > 0 {
                s.sequence++
                retained := removeInterval(
                    block.valid,
                    invalidRange,
                )

                if len(retained) == 0 {
                    if err := os.Remove(block.path); err != nil &&
                        !os.IsNotExist(err) {
                        previousValid := append([]byteInterval(nil), block.valid...)
                        block.valid = nil
                        for _, interval := range previousValid {
                            evicted = append(evicted, byteInterval{
                                start: blockIndex*storeBlockSize + interval.start,
                                end: blockIndex*storeBlockSize + interval.end,
                            })
                        }
                        s.signalLocked()
    revision := s.sequence
    s.mu.Unlock()
    s.notifyEvictions(asset, evicted, revision)
                        return err
                    }
                    delete(blocks, blockIndex)
                    s.allocated -= storeBlockSize
                } else if err := s.rewriteBlockLocked(block, retained); err != nil {
                    previousValid := append([]byteInterval(nil), block.valid...)
                    removeErr := os.Remove(block.path)
                    if removeErr != nil && !os.IsNotExist(removeErr) {
                        block.valid = nil
                        for _, interval := range previousValid {
                            evicted = append(evicted, byteInterval{
                                start: blockIndex*storeBlockSize + interval.start,
                                end: blockIndex*storeBlockSize + interval.end,
                            })
                        }
                        s.signalLocked()
    revision := s.sequence
    s.mu.Unlock()
    s.notifyEvictions(asset, evicted, revision)
                        return fmt.Errorf("failed to rewrite invalidated torrent block: %v; failed to remove it: %w", err, removeErr)
                    }
                    delete(blocks, blockIndex)
                    s.allocated -= storeBlockSize
                    removed = previousValid
                }

                for _, interval := range removed {
                    evicted = append(
                        evicted,
                        byteInterval{
                            start: blockIndex*storeBlockSize + interval.start,
                            end: blockIndex*storeBlockSize + interval.end,
                        },
                    )
                }
            }
        }

        cursor = segmentEnd
    }

    s.signalLocked()
    revision := s.sequence
    s.mu.Unlock()
    s.notifyEvictions(asset, evicted, revision)
    return nil
}

func (s *blockStore) ensureBlockLocked(
	asset string,
	index int64,
) (*storeBlock, error) {
	if s.closed {
		return nil, errors.New("torrent playback storage is closed")
	}

	demand, hasDemand := s.demand[asset]
	blockStart := index * storeBlockSize
	blockEnd := blockStart + storeBlockSize

	if !hasDemand ||
		blockEnd <= demand.start ||
		blockStart >= demand.end {
		return nil, errors.New("torrent block is outside the active playback window")
	}

	blocks := s.assets[asset]
	if blocks == nil {
		blocks = make(map[int64]*storeBlock)
		s.assets[asset] = blocks
	}
	if block := blocks[index]; block != nil {
		return block, nil
	}

	for s.allocated+storeBlockSize > s.maxBytes {
		victimAsset, victimIndex, ok :=
			s.selectVictimLocked()
		if !ok {
			return nil, errors.New("shared torrent storage budget exhausted")
		}

        victim := s.assets[victimAsset][victimIndex]
        if victim == nil {
            continue
        }

        if err := os.Remove(victim.path); err != nil && !os.IsNotExist(err) {
            return nil, err
        }
        s.sequence++
        delete(s.assets[victimAsset], victimIndex)
        s.allocated -= storeBlockSize

        for _, valid := range victim.valid {
            start := victimIndex*storeBlockSize + valid.start
            end := victimIndex*storeBlockSize + valid.end

            generation := s.sequence
            if s.onAttributionEvict != nil {
                s.onAttributionEvict(victimAsset, start, end, generation)
            }
            if s.onEvict != nil {
                callback := s.onEvict
                go callback(victimAsset, start, end, generation)
            }
        }
	}

	directory := filepath.Join(
		s.root,
		hashAssetKey(asset),
	)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}

	path := filepath.Join(
		directory,
		fmt.Sprintf("%016x.blk", index),
	)
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR|os.O_TRUNC,
		0o600,
	)
	if err != nil {
		return nil, err
	}
	if err := file.Truncate(storeBlockSize); err != nil {
		file.Close()
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}

	s.sequence++
	block := &storeBlock{
		path:       path,
		lastAccess: s.sequence,
	}
	blocks[index] = block
	s.allocated += storeBlockSize
	return block, nil
}

func (s *blockStore) selectVictimLocked() (
	string,
	int64,
	bool,
) {
	type victim struct {
		asset    string
		index    int64
		offset   int64
		priority int
		access   uint64
	}

	var candidates []victim
	for asset, blocks := range s.assets {
		current, hasCurrent := s.currentDemand[asset]
		forward, hasForward := s.forwardDemand[asset]
		recent := s.recentDemand[asset]

		for index, block := range blocks {
			blockStart := index * storeBlockSize
			blockEnd := blockStart + storeBlockSize

			if hasCurrent &&
				blockEnd > current.start &&
				blockStart < current.end {
				continue
			}

			priority := 0
			if hasForward &&
				blockEnd > forward.start &&
				blockStart < forward.end {
				priority = 2
			} else {
				for recentIndex, interval := range recent {
					if blockEnd > interval.start && blockStart < interval.end {
						priority = 1
						if recentIndex == 0 {
							break
						}
					}
				}
			}

			candidates = append(candidates, victim{
				asset:    asset,
				index:    index,
				offset:   blockStart,
				priority: priority,
				access:   block.lastAccess,
			})
		}
	}

	if len(candidates) == 0 {
		return "", 0, false
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].priority != candidates[j].priority {
			return candidates[i].priority < candidates[j].priority
		}
		if candidates[i].priority == 2 &&
			candidates[i].offset != candidates[j].offset {
			return candidates[i].offset < candidates[j].offset
		}
		return candidates[i].access < candidates[j].access
	})
	return candidates[0].asset, candidates[0].index, true
}

func (s *blockStore) diskIOBytes() uint64 {

    return s.diskIO.Load()
}

func (s *blockStore) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}

	s.closed = true
	root := s.root
	s.assets = make(map[string]map[int64]*storeBlock)
	s.demand = make(map[string]byteInterval)
	s.currentDemand = make(map[string]byteInterval)
	s.forwardDemand = make(map[string]byteInterval)
	s.demandGeneration = make(map[string]uint64)
	s.recentDemand = make(map[string][]byteInterval)
	s.allocated = 0
	s.signalLocked()
	s.mu.Unlock()

	_ = os.RemoveAll(root)
}

func (s *blockStore) signalLocked() {
	close(s.notify)
	s.notify = make(chan struct{})
}

func hashAssetKey(
	value string,
) string {
	sum := sha256.Sum256(
		[]byte(value),
	)
	return hex.EncodeToString(
		sum[:],
	)
}

func addInterval(
	intervals []byteInterval,
	add byteInterval,
) []byteInterval {
	if add.end <= add.start {
		return intervals
	}

	all := append(
		append(
			[]byteInterval{},
			intervals...,
		),
		add,
	)
	sort.Slice(
		all,
		func(i, j int) bool {
			return all[i].start < all[j].start
		},
	)

	result := make([]byteInterval, 0, len(all))
	for _, interval := range all {
		if len(result) == 0 ||
			result[len(result)-1].end < interval.start {
			result = append(result, interval)
			continue
		}
		if interval.end > result[len(result)-1].end {
			result[len(result)-1].end = interval.end
		}
	}
	return result
}

func removeInterval(
	intervals []byteInterval,
	remove byteInterval,
) []byteInterval {
	result := make([]byteInterval, 0, len(intervals))

	for _, interval := range intervals {
		if interval.end <= remove.start ||
			interval.start >= remove.end {
			result = append(result, interval)
			continue
		}

		if interval.start < remove.start {
			result = append(
				result,
				byteInterval{
					start: interval.start,
					end:   remove.start,
				},
			)
		}
		if interval.end > remove.end {
			result = append(
				result,
				byteInterval{
					start: remove.end,
					end:   interval.end,
				},
			)
		}
	}
	return result
}

func coversRange(
	intervals []byteInterval,
	start int64,
	end int64,
) bool {
	if end <= start {
		return true
	}

	cursor := start
	for _, interval := range intervals {
		if interval.end <= cursor {
			continue
		}
		if interval.start > cursor {
			return false
		}
		if interval.end > cursor {
			cursor = interval.end
		}
		if cursor >= end {
			return true
		}
	}
	return false
}

func coveredLength(
	intervals []byteInterval,
	start int64,
	end int64,
) int64 {
	if end <= start {
		return 0
	}

	var total int64
	for _, interval := range intervals {
		overlapStart := maxInt64(
			start,
			interval.start,
		)
		overlapEnd := minInt64(
			end,
			interval.end,
		)
		if overlapEnd > overlapStart {
			total += overlapEnd - overlapStart
		}
	}
	return total
}

func subtractCovered(
	target byteInterval,
	covered []byteInterval,
) []byteInterval {
	if target.end <= target.start {
		return nil
	}

	var result []byteInterval
	cursor := target.start

	for _, interval := range covered {
		if interval.end <= cursor {
			continue
		}
		if interval.start >= target.end {
			break
		}

		if interval.start > cursor {
			result = append(
				result,
				byteInterval{
					start: cursor,
					end:   minInt64(interval.start, target.end),
				},
			)
		}

		if interval.end > cursor {
			cursor = interval.end
		}
		if cursor >= target.end {
			break
		}
	}

	if cursor < target.end {
		result = append(
			result,
			byteInterval{
				start: cursor,
				end:   target.end,
			},
		)
	}

	return result
}
