package build

import (
	"github.com/google/gopacket"
	"io"
	"strings"
	"fmt"
	"strconv"
	"math/big"
	"time"

	"github.com/40t/go-sniffer/plugSrc/redis/build/go-redis-internal/proto"
	"github.com/google/gopacket/tcpassembly/tcpreader"
	"github.com/secmask/go-redisproto"
)

type Redis struct {
	port int
	version string
}

const (
	Port       int = 6379
	Version string = "0.1"
	CmdPort string = "-p"
)

var redis = &Redis {
	port:Port,
	version:Version,
}

func NewInstance() *Redis{
	return redis
}

func (red Redis) ResolveStream(net, transport gopacket.Flow, r io.Reader) {
	if strings.EqualFold(transport.Src().String(), strconv.Itoa(red.port)) {
		resolveServerPacket(net, transport, r)
	} else {
		resolveClientPacket(net, transport, r)
	}
}

func resolveClientPacket(net, transport gopacket.Flow, r io.Reader) {
	defer tcpreader.DiscardBytesToEOF(r)
	parser := redisproto.NewParser(r)
	for {
		req, err := parser.ReadCommand()
		fmt.Printf("\n%s %s:%s->%s:%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), net.Src().String(), transport.Src().String(), net.Dst().String(), transport.Dst().String())
		if err == io.EOF {
			fmt.Println("# End of stream")
			return
		} else if err != nil {
			fmt.Println("# Error reading stream", ":", err)
			return
		} else {
			var s string
			for i := 0; i <= req.ArgCount(); i++ {
				s += string(req.Get(i)) + " "
			}
			fmt.Println(s)
		}
	}
}

func resolveServerPacket(net, transport gopacket.Flow, r io.Reader) {
	defer tcpreader.DiscardBytesToEOF(r)
	reader := proto.NewReader(r)
	for {
		resp, err := reader.ReadReply()
		fmt.Printf("\n%s %s:%s->%s:%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), net.Src().String(), transport.Src().String(), net.Dst().String(), transport.Dst().String())
		if err == io.EOF {
			fmt.Println("# End of stream")
			return
		} else if _, ok := err.(proto.RedisError); ok {
			fmt.Print(dumpResponse(err, ""))
		} else if err != nil {
			fmt.Println("# Error reading stream", ":", err)
			return
		} else {
			fmt.Print(dumpResponse(resp, ""))
		}
	}
}

func dumpResponse(resp interface{}, spaces string) string {
	str := ""
	switch resp := resp.(type) {
	case nil:
		str += fmt.Sprintf("%s\n", "(Null)")
	case int64:
		str += fmt.Sprintf("%s%+v\n", "(Number)", resp)
	case float64:
		str += fmt.Sprintf("%s%+v\n", "(Double)", resp)
	case bool:
		str += fmt.Sprintf("%s%+v\n", "(Boolean)", resp)
	case *big.Int:
		str += fmt.Sprintf("%s%+v\n", "(BigNumber)", resp)
	case string:
		str += fmt.Sprintf("%s%+v\n", "(String)", resp)
	case []interface{}:
		arr := resp
		str += fmt.Sprintf("%s%+v\n", "(Array)", len(arr))
		for i, v := range arr {
			s := strconv.Itoa(i) + " "
			str += fmt.Sprintf("%s%s", spaces, s)
			str += dumpResponse(v, spaces+strings.Repeat(" ", len(s)))
		}
		str += "\n"
	case map[interface{}]interface{}:
		m := resp
		str += fmt.Sprintf("%s%+v\n", "(Map)", len(m))
		for k, v := range m {
			s := fmt.Sprintf("%v", k) + ":"
			str += fmt.Sprintf("%s%s", spaces, s)
			str += dumpResponse(v, spaces+strings.Repeat(" ", len(s)))
		}
		str += "\n"
	case proto.RedisError:
		if resp == proto.Nil {
			str += fmt.Sprintf("%s\n", "(Null)")
		} else {
			str += fmt.Sprintf("%s%+v\n", "(Error)", resp)
		}

	default:
		str += fmt.Sprintf("%s%+v\n", "(Unknow)", resp)
	}

	return str
}

/**
	SetOption
 */
func (red *Redis) SetFlag(flg []string)  {
	c := len(flg)
	if c == 0 {
		return
	}
	if c >> 1 != 1 {
		panic("ERR : Redis num of params")
	}
	for i:=0;i<c;i=i+2 {
		key := flg[i]
		val := flg[i+1]

		switch key {
		case CmdPort:
			port, err := strconv.Atoi(val);
			redis.port = port
			if err != nil {
				panic("ERR : Port error")
			}
			if port < 0 || port > 65535 {
				panic("ERR : Port(0-65535)")
			}
			break
		default:
			panic("ERR : redis's params")
		}
	}
}

/**
	BPFFilter
 */
func (red *Redis) BPFFilter() string {
	return "tcp and port "+strconv.Itoa(redis.port)
}

/**
	Version
 */
func (red *Redis) Version() string {
	return red.version
}

