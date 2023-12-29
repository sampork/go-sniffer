package build

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/google/gopacket"
	"io"
	"strconv"
	"time"

	"github.com/40t/go-sniffer/plugSrc/mongodb/build/FerretDB-internal/wire"
	"github.com/google/gopacket/tcpassembly/tcpreader"
)

const (
	Port = 27017
	Version = "0.1"
	CmdPort = "-p"
)

type Mongodb struct {
	port    int
	version string
}

var mongodbInstance *Mongodb

func NewInstance() *Mongodb {
	if mongodbInstance == nil {
		mongodbInstance = &Mongodb{
			port   :Port,
			version:Version,
		}
	}
	return mongodbInstance
}

func (m *Mongodb) SetFlag(flg []string)  {
	c := len(flg)
	if c == 0 {
		return
	}
	if c >> 1 != 1 {
		panic("ERR : Mongodb Number of parameters")
	}
	for i:=0;i<c;i=i+2 {
		key := flg[i]
		val := flg[i+1]

		switch key {
		case CmdPort:
			p, err := strconv.Atoi(val);
			if err != nil {
				panic("ERR : port")
			}
			mongodbInstance.port = p
			if p < 0 || p > 65535 {
				panic("ERR : port(0-65535)")
			}
			break
		default:
			panic("ERR : mysql's params")
		}
	}
}

func (m *Mongodb) BPFFilter() string {
	return "tcp and port "+strconv.Itoa(m.port);
}

func (m *Mongodb) Version() string {
	return m.version
}

func (m *Mongodb) ResolveStream(net, transport gopacket.Flow, r io.Reader) {
	defer tcpreader.DiscardBytesToEOF(r)
	buf := bufio.NewReader(r)
	for {
		header, body, err := wire.ReadMessage(buf)
		fmt.Printf("\n%s %s:%s->%s:%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), net.Src().String(), transport.Src().String(), net.Dst().String(), transport.Dst().String())
		if err == io.EOF || errors.Is(err, wire.ErrZeroRead) {
			fmt.Println("# End of stream")
			return
		} else if err != nil {
			fmt.Println("# Error reading stream", ":", err)
			return
		} else {
			fmt.Println(header)
			fmt.Println(body)
		}
	}
}
