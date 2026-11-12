package torr

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"golang.org/x/net/proxy"
)

const (
	lpdIPv4Address = "239.192.152.143:6771"
	lpdIPv6Address = "[ff15::efc0:988f]:6771"
	lpdInterval    = 5 * time.Minute
)

type proxyContextDialer interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

type proxyDialerAdapter struct {
	dialer proxy.Dialer
}

func (d proxyDialerAdapter) DialContext(
	ctx context.Context,
	network string,
	address string,
) (net.Conn, error) {
	if contextDialer, ok := d.dialer.(proxy.ContextDialer); ok {
		return contextDialer.DialContext(ctx, network, address)
	}

	type result struct {
		conn net.Conn
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		conn, err := d.dialer.Dial(network, address)
		resultCh <- result{conn: conn, err: err}
	}()

	select {
	case <-ctx.Done():
		go func() {
			res := <-resultCh
			if res.conn != nil {
				_ = res.conn.Close()
			}
		}()
		return nil, ctx.Err()
	case res := <-resultCh:
		return res.conn, res.err
	}
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

type httpConnectDialer struct {
	proxyURL *url.URL
}

func (d *httpConnectDialer) DialContext(
	ctx context.Context,
	_ string,
	address string,
) (net.Conn, error) {
	proxyAddress := d.proxyURL.Host
	if _, _, err := net.SplitHostPort(proxyAddress); err != nil {
		port := "80"
		if d.proxyURL.Scheme == "https" {
			port = "443"
		}
		proxyAddress = net.JoinHostPort(d.proxyURL.Hostname(), port)
	}

	netDialer := &net.Dialer{}
	conn, err := netDialer.DialContext(ctx, "tcp", proxyAddress)
	if err != nil {
		return nil, err
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = conn.Close()
		}
	}()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}

	if d.proxyURL.Scheme == "https" {
		tlsConn := tls.Client(conn, &tls.Config{
			ServerName: d.proxyURL.Hostname(),
			MinVersion: tls.VersionTLS12,
		})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			return nil, err
		}
		conn = tlsConn
	}

	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: address},
		Host:   address,
		Header: make(http.Header),
	}
	if d.proxyURL.User != nil {
		password, _ := d.proxyURL.User.Password()
		token := base64.StdEncoding.EncodeToString(
			[]byte(d.proxyURL.User.Username() + ":" + password),
		)
		req.Header.Set("Proxy-Authorization", "Basic "+token)
	}

	if err := req.Write(conn); err != nil {
		return nil, err
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("proxy CONNECT failed: %s", response.Status)
	}

	closeOnError = false
	if reader.Buffered() == 0 {
		return conn, nil
	}
	return &bufferedConn{Conn: conn, reader: reader}, nil
}

type socks4Dialer struct {
	proxyURL *url.URL
}

func (d *socks4Dialer) DialContext(
	ctx context.Context,
	_ string,
	address string,
) (net.Conn, error) {
	proxyAddress := d.proxyURL.Host
	if _, _, err := net.SplitHostPort(proxyAddress); err != nil {
		proxyAddress = net.JoinHostPort(d.proxyURL.Hostname(), "1080")
	}

	host, portString, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	port, err := strconv.ParseUint(portString, 10, 16)
	if err != nil {
		return nil, err
	}

	netDialer := &net.Dialer{}
	conn, err := netDialer.DialContext(ctx, "tcp", proxyAddress)
	if err != nil {
		return nil, err
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = conn.Close()
		}
	}()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}

	request := []byte{0x04, 0x01, byte(port >> 8), byte(port)}
	var domain string
	ip := net.ParseIP(host)
	if d.proxyURL.Scheme == "socks4" {
		if ip == nil || ip.To4() == nil {
			addresses, err := net.DefaultResolver.LookupIP(ctx, "ip4", host)
			if err != nil || len(addresses) == 0 {
				return nil, fmt.Errorf("SOCKS4 target has no IPv4 address: %s", host)
			}
			ip = addresses[0]
		}
		request = append(request, ip.To4()...)
	} else {
		if ip != nil && ip.To4() != nil {
			request = append(request, ip.To4()...)
		} else {
			request = append(request, 0, 0, 0, 1)
			domain = host
		}
	}

	if d.proxyURL.User != nil {
		request = append(request, []byte(d.proxyURL.User.Username())...)
	}
	request = append(request, 0)
	if domain != "" {
		request = append(request, []byte(domain)...)
		request = append(request, 0)
	}

	if _, err := conn.Write(request); err != nil {
		return nil, err
	}
	response := make([]byte, 8)
	if _, err := io.ReadFull(conn, response); err != nil {
		return nil, err
	}
	if response[1] != 0x5a {
		return nil, fmt.Errorf("SOCKS4 proxy rejected connection: 0x%02x", response[1])
	}

	closeOnError = false
	return conn, nil
}

func newProxyDialer(rawURL string) (proxyContextDialer, *url.URL, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid proxy URL: %w", err)
	}
	if parsedURL.Hostname() == "" {
		return nil, nil, fmt.Errorf("proxy URL has no host")
	}

	switch parsedURL.Scheme {
	case "socks5", "socks5h":
		if parsedURL.Port() == "" {
			parsedURL.Host = net.JoinHostPort(parsedURL.Hostname(), "1080")
		}
		dialer, err := proxy.FromURL(parsedURL, proxy.Direct)
		if err != nil {
			return nil, nil, err
		}
		return proxyDialerAdapter{dialer: dialer}, parsedURL, nil
	case "socks4", "socks4a":
		return &socks4Dialer{proxyURL: parsedURL}, parsedURL, nil
	case "http", "https":
		return &httpConnectDialer{proxyURL: parsedURL}, parsedURL, nil
	default:
		return nil, nil, fmt.Errorf(
			"unsupported proxy protocol: %s (supported: http, https, socks4, socks4a, socks5, socks5h)",
			parsedURL.Scheme,
		)
	}
}

func configureOfficialTorrentProxy(
	config *torrent.ClientConfig,
	rawURL string,
	mode string,
) ([]torrent.Dialer, error) {
	if rawURL == "" {
		return nil, nil
	}
	if mode == "" {
		mode = "tracker"
	}

	dialer, _, err := newProxyDialer(rawURL)
	if err != nil {
		return nil, err
	}

	switch mode {
	case "tracker":
		config.TrackerDialContext = dialer.DialContext
		return nil, nil

	case "peers":
		config.DialForPeerConns = false
		config.DisableWebtorrent = true
		return peerProxyDialers(config, dialer), nil

	case "full":
		config.DialForPeerConns = false
		config.AcceptPeerConnections = false
		config.DisableUTP = true
		config.DisableWebtorrent = true
		config.NoDHT = true
		config.NoDefaultPortForwarding = true
		config.TrackerDialContext = dialer.DialContext
		config.HTTPDialContext = dialer.DialContext
		config.TrackerListenPacket = func(string, string) (net.PacketConn, error) {
			return nil, fmt.Errorf("UDP trackers are disabled in full proxy mode")
		}
		return peerProxyDialers(config, dialer), nil

	default:
		return nil, fmt.Errorf(
			"unsupported proxy mode: %s (supported: tracker, peers, full)",
			mode,
		)
	}
}

func peerProxyDialers(
	config *torrent.ClientConfig,
	dialer proxyContextDialer,
) []torrent.Dialer {
	result := []torrent.Dialer{
		torrent.NetworkDialer{
			Network: "tcp4",
			Dialer:  dialer,
		},
	}
	if !config.DisableIPv6 {
		result = append(
			result,
			torrent.NetworkDialer{
				Network: "tcp6",
				Dialer:  dialer,
			},
		)
	}
	return result
}

type localPeerDiscovery struct {
	bt      *BTServer
	sockets []*localPeerDiscoverySocket
	done    chan struct{}
	once    sync.Once
	wg      sync.WaitGroup
}

type localPeerDiscoverySocket struct {
	network   string
	host      string
	listener  *net.UDPConn
	publisher *net.UDPConn
}

func startLocalPeerDiscovery(
	bt *BTServer,
	ipv6 bool,
) (*localPeerDiscovery, error) {
	discovery := &localPeerDiscovery{
		bt:   bt,
		done: make(chan struct{}),
	}

	ipv4, err := newLocalPeerDiscoverySocket("udp4", lpdIPv4Address)
	if err != nil {
		return nil, err
	}
	discovery.sockets = append(discovery.sockets, ipv4)

	if ipv6 {
		if ipv6Socket, err := newLocalPeerDiscoverySocket("udp6", lpdIPv6Address); err == nil {
			discovery.sockets = append(discovery.sockets, ipv6Socket)
		}
	}

	for _, socket := range discovery.sockets {
		discovery.wg.Add(1)
		go discovery.receive(socket)
	}
	discovery.wg.Add(1)
	go discovery.announceLoop()
	return discovery, nil
}

func newLocalPeerDiscoverySocket(
	network string,
	host string,
) (*localPeerDiscoverySocket, error) {
	address, err := net.ResolveUDPAddr(network, host)
	if err != nil {
		return nil, err
	}
	listener, err := net.ListenMulticastUDP(network, nil, address)
	if err != nil {
		return nil, err
	}
	publisher, err := net.DialUDP(network, nil, address)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	return &localPeerDiscoverySocket{
		network:   network,
		host:      host,
		listener:  listener,
		publisher: publisher,
	}, nil
}

func (d *localPeerDiscovery) announceLoop() {
	defer d.wg.Done()
	d.announce()
	ticker := time.NewTicker(lpdInterval)
	defer ticker.Stop()
	for {
		select {
		case <-d.done:
			return
		case <-ticker.C:
			d.announce()
		}
	}
}

func (d *localPeerDiscovery) announce() {
	port, hashes := d.snapshot()
	if port <= 0 || len(hashes) == 0 {
		return
	}

	for _, socket := range d.sockets {
		for _, hash := range hashes {
			packet := fmt.Sprintf(
				"BT-SEARCH * HTTP/1.1\r\nHost: %s\r\nPort: %d\r\nInfohash: %s\r\n\r\n",
				socket.host,
				port,
				strings.ToUpper(hash),
			)
			_, _ = socket.publisher.Write([]byte(packet))
		}
	}
}

func (d *localPeerDiscovery) snapshot() (int, []string) {
	d.bt.mu.RLock()
	defer d.bt.mu.RUnlock()
	if d.bt.client == nil {
		return 0, nil
	}

	port := d.bt.client.LocalPort()
	hashes := make([]string, 0, len(d.bt.torrents))
	for hash, item := range d.bt.torrents {
		if item == nil || item.Torrent == nil || torrentIsPrivate(item.Torrent) {
			continue
		}
		hashes = append(hashes, hash.HexString())
	}
	return port, hashes
}

func (d *localPeerDiscovery) receive(socket *localPeerDiscoverySocket) {
	defer d.wg.Done()
	buffer := make([]byte, 2048)
	for {
		n, from, err := socket.listener.ReadFromUDP(buffer)
		if err != nil {
			select {
			case <-d.done:
				return
			default:
				continue
			}
		}
		d.handlePacket(socket, buffer[:n], from)
	}
}

func (d *localPeerDiscovery) handlePacket(
	socket *localPeerDiscoverySocket,
	data []byte,
	from *net.UDPAddr,
) {
	request, err := http.ReadRequest(bufio.NewReader(strings.NewReader(string(data))))
	if err != nil || request.Method != "BT-SEARCH" {
		return
	}
	port, err := strconv.Atoi(request.Header.Get("Port"))
	if err != nil || port <= 0 || port > 65535 {
		return
	}

	if publisherAddress, ok := socket.publisher.LocalAddr().(*net.UDPAddr); ok &&
		publisherAddress.IP != nil &&
		from.IP.Equal(publisherAddress.IP) {
		d.bt.mu.RLock()
		client := d.bt.client
		d.bt.mu.RUnlock()
		if client != nil && port == client.LocalPort() {
			return
		}
	}

	for _, value := range request.Header.Values("Infohash") {
		var hash metainfo.Hash
		if err := hash.FromHexString(strings.ToLower(value)); err != nil {
			continue
		}
		d.addPeer(hash, from.IP, port)
	}
}

func (d *localPeerDiscovery) addPeer(
	hash metainfo.Hash,
	ip net.IP,
	port int,
) {
	d.bt.mu.RLock()
	item := d.bt.torrents[hash]
	d.bt.mu.RUnlock()
	if item == nil || item.Torrent == nil || torrentIsPrivate(item.Torrent) {
		return
	}

	item.Torrent.AddPeers([]torrent.PeerInfo{
		{
			Addr: &net.TCPAddr{
				IP:   append(net.IP(nil), ip...),
				Port: port,
			},
			Source: torrent.PeerSource("L"),
		},
	})
}

func torrentIsPrivate(item *torrent.Torrent) bool {
	info := item.Info()
	return info != nil && info.Private != nil && *info.Private
}

func (d *localPeerDiscovery) Close() {
	d.once.Do(func() {
		close(d.done)
		for _, socket := range d.sockets {
			_ = socket.listener.Close()
			_ = socket.publisher.Close()
		}
		d.wg.Wait()
	})
}
