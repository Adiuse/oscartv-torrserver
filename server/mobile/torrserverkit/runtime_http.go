package torrserverkit

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

const streamReadChunkSize int64 = 256 << 10

func (r *playbackRuntime) mountRoutes(
	router *gin.Engine,
) {
	router.GET(
		"/oscartv/torrent/:playbackId/*name",
		r.handleStream,
	)
	router.HEAD(
		"/oscartv/torrent/:playbackId/*name",
		r.handleStream,
	)
}

func (r *playbackRuntime) handleStream(
	c *gin.Context,
) {
	playbackID := c.Param("playbackId")
	session, err := r.session(playbackID)
	if err != nil {
		c.Status(http.StatusNotFound)
		return
	}

	length := session.targetLength
	if length <= 0 {
		c.Status(http.StatusServiceUnavailable)
		return
	}

	start, end, partial, err := parseByteRange(
		c.GetHeader("Range"),
		length,
	)
	if err != nil {
		c.Header(
			"Content-Range",
			fmt.Sprintf("bytes */%d", length),
		)
		c.Status(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	c.Header("Accept-Ranges", "bytes")
	c.Header("Cache-Control", "no-store")
	c.Header(
		"Content-Type",
		contentTypeForExtension(
			session.config.ContainerExtension,
		),
	)
	c.Header(
		"Content-Length",
		strconv.FormatInt(
			end-start+1,
			10,
		),
	)

	if partial {
		c.Header(
			"Content-Range",
			fmt.Sprintf(
				"bytes %d-%d/%d",
				start,
				end,
				length,
			),
		)
	}

	if c.Request.Method == http.MethodHead {
		if partial {
			c.Status(http.StatusPartialContent)
		} else {
			c.Status(http.StatusOK)
		}
		return
	}

	ctx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()
	demandGeneration := session.beginDemandRequest(start, cancel)
	defer session.endDemandRequest(demandGeneration)
	position := start
	firstWrite := true

	for position <= end {
        chunkLength := minInt64(
            streamReadChunkSize,
            end-position+1,
        )
        if err := session.setDemandForGeneration(
            position,
            chunkLength,
            demandGeneration,
        ); err != nil {
			if firstWrite &&
				ctx.Err() == nil &&
				session.currentDemandGeneration() == demandGeneration {
				c.Status(http.StatusServiceUnavailable)
			}
			return
		}

		buffer := make(
			[]byte,
			int(chunkLength),
		)
		if err := session.store.waitRead(
			ctx,
			session.targetAssetKey,
			position,
			buffer,
		); err != nil {
			if firstWrite &&
				ctx.Err() == nil &&
				session.currentDemandGeneration() == demandGeneration {
				c.Status(http.StatusServiceUnavailable)
			}
			return
		}

		if session.currentDemandGeneration() != demandGeneration {
			return
		}

		if firstWrite {
			if partial {
				c.Status(http.StatusPartialContent)
			} else {
				c.Status(http.StatusOK)
			}
			firstWrite = false
		}

		n, writeErr := c.Writer.Write(buffer)
		if n > 0 {
			session.recordServed(
				position,
				position+int64(n),
			)
			position += int64(n)
		}

		if writeErr != nil {
			return
		}
		if n != len(buffer) {
			return
		}

		c.Writer.Flush()
	}
}

func parseByteRange(
	header string,
	length int64,
) (
	start int64,
	end int64,
	partial bool,
	err error,
) {
	if length <= 0 {
		return 0, 0, false, errorsNewRange()
	}

	if strings.TrimSpace(header) == "" {
		return 0, length - 1, false, nil
	}

	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false, errorsNewRange()
	}

	value := strings.TrimSpace(
		strings.TrimPrefix(
			header,
			"bytes=",
		),
	)
	if strings.Contains(value, ",") {
		return 0, 0, false, errorsNewRange()
	}

	parts := strings.SplitN(value, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false, errorsNewRange()
	}

	if parts[0] == "" {
		suffix, parseErr := strconv.ParseInt(
			parts[1],
			10,
			64,
		)
		if parseErr != nil || suffix <= 0 {
			return 0, 0, false, errorsNewRange()
		}
		if suffix > length {
			suffix = length
		}
		return length - suffix, length - 1, true, nil
	}

	start, err = strconv.ParseInt(
		parts[0],
		10,
		64,
	)
	if err != nil ||
		start < 0 ||
		start >= length {
		return 0, 0, false, errorsNewRange()
	}

	if parts[1] == "" {
		return start, length - 1, true, nil
	}

	end, err = strconv.ParseInt(
		parts[1],
		10,
		64,
	)
	if err != nil ||
		end < start {
		return 0, 0, false, errorsNewRange()
	}
	if end >= length {
		end = length - 1
	}

	return start, end, true, nil
}

func errorsNewRange() error {
	return fmt.Errorf("invalid byte range")
}

func contentTypeForExtension(
	extension string,
) string {
	switch strings.ToLower(extension) {
	case "mkv":
		return "video/x-matroska"
	case "mp4":
		return "video/mp4"
	default:
		return "application/octet-stream"
	}
}

func waitContext(
	ctx context.Context,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

var _ io.Reader = nil
