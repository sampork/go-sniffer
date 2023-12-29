package build

import (
	"github.com/google/gopacket"
	"io"
	"strconv"
	"fmt"
	"os"
	"bufio"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/google/gopacket/tcpassembly/tcpreader"
)

const (
	Port       = 80
	Version    = "0.1"
)

const (
	CmdPort    = "-p"
)

type H struct {
	port       int
	version    string
}

var hp *H

func NewInstance() *H {
	if hp == nil {
		hp = &H{
			port   :Port,
			version:Version,
		}
	}
	return hp
}

func (m *H) ResolveStream(net, transport gopacket.Flow, buf io.Reader) {
	if transport.Src().String() == strconv.Itoa(m.port) {
		resolveServerPacket(net, transport, buf)
	} else {
		resolveClientPacket(net, transport, buf)
	}
}

func (m *H) BPFFilter() string {
	return "tcp and port "+strconv.Itoa(m.port);
}

func (m *H) Version() string {
	return Version
}

func (m *H) SetFlag(flg []string)  {

	c := len(flg)

	if c == 0 {
		return
	}
	if c >> 1 == 0 {
		fmt.Println("ERR : Http Number of parameters")
		os.Exit(1)
	}
	for i:=0;i<c;i=i+2 {
		key := flg[i]
		val := flg[i+1]

		switch key {
		case CmdPort:
			port, err := strconv.Atoi(val);
			m.port = port
			if err != nil {
				panic("ERR : port")
			}
			if port < 0 || port > 65535 {
				panic("ERR : port(0-65535)")
			}
			break
		default:
			panic("ERR : mysql's params")
		}
	}
}

func resolveServerPacket(net, transport gopacket.Flow, r io.Reader) {
	defer tcpreader.DiscardBytesToEOF(r)
	buf := bufio.NewReader(r)
	for {
		resp, err := http.ReadResponse(buf, nil)
		fmt.Printf("\n%s %s:%s->%s:%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), net.Src().String(), transport.Src().String(), net.Dst().String(), transport.Dst().String())
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			fmt.Println("# End of stream")
			return
		} else if err != nil {
			fmt.Println("# Error reading stream", ":", err)
			return
		} else {
			defer resp.Body.Close()
			printResponse(net, transport, resp)
		}
	}
}

func resolveClientPacket(net, transport gopacket.Flow, r io.Reader) {
	defer tcpreader.DiscardBytesToEOF(r)
	buf := bufio.NewReader(r)
	for {
		req, err := http.ReadRequest(buf)
		fmt.Printf("\n%s %s:%s->%s:%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), net.Src().String(), transport.Src().String(), net.Dst().String(), transport.Dst().String())
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			fmt.Println("# End of stream")
			return
		} else if err != nil {
			fmt.Println("# Error reading stream", ":", err)
			return
		} else {
			defer req.Body.Close()
			printRequest(net, transport, req)
		}
	}
}

func printRequest(net, transport gopacket.Flow, req *http.Request) {
	dump, err := httputil.DumpRequest(req, true)
	if err != nil {
		fmt.Println(err)
	} else {
		s := string(dump)
		fmt.Println(s)
	}
}

func printResponse(net, transport gopacket.Flow, resp *http.Response) {
	dump, err := httputil.DumpResponse(resp, true)
	if err != nil {
		fmt.Println(err)
	} else {
		s := string(dump)
		fmt.Println(s)
	}
}
