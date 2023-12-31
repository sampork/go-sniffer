package build

import (
	"github.com/google/gopacket"
	"io"
	"bytes"
	"errors"
	"log"
	"strconv"
	"sync"
	"time"
	"fmt"
	"encoding/binary"
	"strings"
	"os"

	"github.com/google/gopacket/tcpassembly/tcpreader"
	mp "github.com/vczyh/mysql-protocol/packet"
	mf "github.com/vczyh/mysql-protocol/flag"
	gm "github.com/XiaoMi/Gaea/mysql"
)

const (
	Port                  = 3306
	ClientDeprecateEof    = false
	Version               = "0.1"
	CmdPort               = "-p"
	CmdClientDeprecateEof = "-c"
)

type Mysql struct {
	port               int
	clientDeprecateEof bool
	version            string
	source             map[string]*stream
}

type stream struct {
	packets chan *packet
	stmtMap map[uint32]*Stmt
}

type packet struct {
	isClientFlow   bool
	net, transport gopacket.Flow
	seq            int
	length         int
	payload        []byte
}

func packetFlowInfo(net, transport gopacket.Flow) string {
	return fmt.Sprintf("\n%s %s:%s->%s:%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), net.Src().String(), transport.Src().String(), net.Dst().String(), transport.Dst().String())
}

var mysql *Mysql
var once sync.Once
func NewInstance() *Mysql {

	once.Do(func() {
		mysql = &Mysql{
			port:               Port,
			clientDeprecateEof: ClientDeprecateEof,
			version:            Version,
			source:             make(map[string]*stream),
		}
	})

	return mysql
}

func (m *Mysql) ResolveStream(net, transport gopacket.Flow, buf io.Reader) {
	defer tcpreader.DiscardBytesToEOF(buf)

	//uuid
	uuid := fmt.Sprintf("%v:%v", net.FastHash(), transport.FastHash())

	//generate resolve's stream
	if _, ok := m.source[uuid]; !ok {

		var newStream = stream{
			packets:make(chan *packet, 100),
			stmtMap:make(map[uint32]*Stmt),
		}

		m.source[uuid] = &newStream
		go newStream.resolve()
	}

	//read bi-directional packet
	//server -> client || client -> server
	for {

		newPacket := m.newPacket(net, transport, buf)

		if newPacket == nil {
			return
		}

		m.source[uuid].packets <- newPacket
	}
}

func (m *Mysql) BPFFilter() string {
	return "tcp and port "+strconv.Itoa(m.port);
}

func (m *Mysql) Version() string {
	return Version
}

func (m *Mysql) SetFlag(flg []string)  {

	c := len(flg)

	if c == 0 {
		return
	}
	if c >> 1 == 0 {
		fmt.Println("ERR : Mysql Number of parameters")
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
		case CmdClientDeprecateEof:
			if val == "true" {
				m.clientDeprecateEof = true
			} else {
				m.clientDeprecateEof = false
			}
		default:
			panic("ERR : mysql's params")
		}
	}
}

func (m *Mysql) newPacket(net, transport gopacket.Flow, r io.Reader) *packet {

	//read packet
	var payload bytes.Buffer
	var seq uint8
	var err error
	if seq, err = m.resolvePacketTo(r, &payload); err == io.EOF {
		fmt.Print(packetFlowInfo(net, transport))
		fmt.Println("# End of stream")
		return nil
	} else if err != nil {
		fmt.Print(packetFlowInfo(net, transport))
		fmt.Println("# Error reading stream", ":", err)
		return nil
	}

	//generate new packet
	var pk = packet{
		net:       net,
		transport: transport,
		seq:       int(seq),
		length:    payload.Len(),
		payload:   payload.Bytes(),
	}
	if transport.Src().String() == strconv.Itoa(m.port) {
		pk.isClientFlow = false
	}else{
		pk.isClientFlow = true
	}

	return &pk
}

func (m *Mysql) resolvePacketTo(r io.Reader, w io.Writer) (uint8, error) {

	header := make([]byte, 4)
	if n, err := io.ReadFull(r, header); err != nil {
		if n == 0 && err == io.EOF {
			return 0, io.EOF
		}
		return 0, errors.New("ERR : Unknown stream")
	}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

	var seq uint8
	seq = header[3]

	if n, err := io.CopyN(w, r, int64(length)); err != nil {
		return 0, errors.New("ERR : Unknown stream")
	} else if n != int64(length) {
		return 0, errors.New("ERR : Unknown stream")
	} else {
		return seq, nil
	}

	return seq, nil
}

func (stm *stream) resolve() {
	for {
		select {
		case packet := <- stm.packets:
			if packet.length != 0 {
				if packet.isClientFlow {
					stm.resolveClientPacket(packet)
				} else {
					stm.resolveServerPacket(packet)
				}
			}
		}
	}
}

func (stm *stream) findStmtPacket (srv chan *packet, seq int) *packet {
	for {
		select {
		case packet, ok := <- stm.packets:
			if !ok {
				return nil
			}
			if packet.seq == seq%255 {
				return packet
			}
		case <-time.After(5 * time.Second):
			return nil
		}
	}
}

func dumpColumn(c *gm.Field) string {
	var sb strings.Builder
	sb.WriteString("Schema: " + string(c.Schema))
	sb.WriteString(", ")

	sb.WriteString("Table: " + string(c.Table))
	sb.WriteString(", ")

	sb.WriteString("Name: " + string(c.Name))
	sb.WriteString(", ")

	charset := fmt.Sprintf("%d", c.Charset)
	name := charset
	var ok bool
	if name, ok = gm.Collations[gm.CollationID(c.Charset)]; ok {
		charset = gm.CollationNameToCharset[name]
	}

	sb.WriteString(fmt.Sprintf("CharSet: [%s,%s]", charset, name))
	sb.WriteString(", ")

	sb.WriteString(fmt.Sprintf("Length: %d", c.ColumnLength))
	sb.WriteString(", ")

	sb.WriteString("Type: " + mf.TableColumnType(c.Type).String())
	sb.WriteString(", ")

	sb.WriteString(fmt.Sprintf("Decimal: %x", c.Decimal))
	return sb.String()
}

func dumpRow(r []interface{}) string {
	values := make([]string, len(r))
	for i, cv := range r {
		switch v := cv.(type) {
		case nil:
			values[i] = "NULL"
		case []byte:
			values[i] = string(v)
		default:
			values[i] = fmt.Sprintf("%+v", v)
		}
	}
	return strings.Join(values, " | ")
}

func (stm *stream) readResult(seq int, isBinary bool) (msg string) {
	serverPacket := stm.findStmtPacket(stm.packets, seq+1)
	if serverPacket == nil {
		log.Println("ERR : Not found stm packet")
		return ""
	}
	seq += 1

	defer func() {
		msg = packetFlowInfo(serverPacket.net, serverPacket.transport) + msg
	}()

	switch serverPacket.payload[0] {
	case 0x00:
		// OK.
		var pos = 1
		l, _, n := LengthEncodedInt(serverPacket.payload[pos:])
		affectedRows := int(l)

		i, _, _ := LengthEncodedInt(serverPacket.payload[pos+n:])
		lastInsertId := int(i)

		f := "%s Effect Row:%s,Last Insert Id:%s"
		msg += fmt.Sprintf(f, OkPacket, strconv.Itoa(affectedRows), strconv.Itoa(lastInsertId))
		return msg
	case 0xff:
		// Err
		errorCode := int(binary.LittleEndian.Uint16(serverPacket.payload[1:3]))
		errorMsg, _ := ReadStringFromByte(serverPacket.payload[4:])

		f := "%s Err code:%s,Err msg:%s"
		msg += fmt.Sprintf(f, ErrorPacket, strconv.Itoa(errorCode), strings.TrimSpace(errorMsg))
		return msg
	case 0xfb:
		// Local infile
		msg += "Local infile unsupported"
		return msg
	}

	var err error
	var count uint64
	if count, err = mp.ParseColumnCount(serverPacket.payload); err != nil {
		msg += fmt.Sprintf("ERR : Parse column count %+v", err)
		return msg
	}
	columnCount := int(count)

	var columns []*gm.Field
	i := 0
columnLoop:
	for {
		serverPacket := stm.findStmtPacket(stm.packets, seq+1)
		if serverPacket == nil {
			msg += "ERR : Not found stm packet"
			return msg
		}
		seq += 1

		switch serverPacket.payload[0] {
		case 0xff:
			// Err
			errorCode := int(binary.LittleEndian.Uint16(serverPacket.payload[1:3]))
			errorMsg, _ := ReadStringFromByte(serverPacket.payload[4:])

			f := "%s Err code:%s,Err msg:%s"
			msg += fmt.Sprintf(f, ErrorPacket, strconv.Itoa(errorCode), strings.TrimSpace(errorMsg))
			return msg
		case 0xfe:
			// EOF
			if i != columnCount {
				msg += fmt.Sprintf("ERR : Column count mismatch n:%d len:%d", columnCount, len(columns))
				return msg
			}
			break columnLoop
		}

		column, err := gm.FieldData(serverPacket.payload).Parse()
		if err != nil {
			msg += fmt.Sprintf("ERR : Parse column %+v", err)
			return msg
		}
		columns = append(columns, column)

		i++

		if mysql.clientDeprecateEof {
			if i == columnCount {
				break
			}
		}
	}

	msg += fmt.Sprintf("(Column)%d\n", len(columns))
	for i, v := range columns {
		msg += fmt.Sprintf("%d) %s\n", i, dumpColumn(v))
	}

	var rows [][]interface{}
rowLoop:
	for {
		serverPacket := stm.findStmtPacket(stm.packets, seq+1)
		if serverPacket == nil {
			msg += "ERR : Not found stm packet"
			return msg
		}
		seq += 1

		switch serverPacket.payload[0] {
		case 0xff:
			// Err
			errorCode := int(binary.LittleEndian.Uint16(serverPacket.payload[1:3]))
			errorMsg, _ := ReadStringFromByte(serverPacket.payload[4:])

			f := "%s Err code:%s,Err msg:%s"
			msg += fmt.Sprintf(f, ErrorPacket, strconv.Itoa(errorCode), strings.TrimSpace(errorMsg))
			return msg
		case 0xfe:
			// EOF
			break rowLoop
		}

		row, err := gm.RowData(serverPacket.payload).Parse(columns, isBinary)
		if err != nil {
			msg += fmt.Sprintf("ERR : Parse row %+v", err)
			return msg
		}

		rows = append(rows, row)
	}

	msg += fmt.Sprintf("(Row)%d\n", len(rows))
	for i, v := range rows {
		msg += fmt.Sprintf("%d) %s\n", i, dumpRow(v))
	}

	return msg
}

func (stm *stream) resolveServerPacket(packet *packet) {

	var msg = packetFlowInfo(packet.net, packet.transport)
	if len(packet.payload) == 0 {
		return
	}
	switch packet.payload[0] {

		case 0xff:
			errorCode  := int(binary.LittleEndian.Uint16(packet.payload[1:3]))
			errorMsg,_ := ReadStringFromByte(packet.payload[4:])

			f := "%s Err code:%s,Err msg:%s"
			msg += fmt.Sprintf(f, ErrorPacket, strconv.Itoa(errorCode), strings.TrimSpace(errorMsg))

		case 0x00:
			var pos = 1
			l, _, n := LengthEncodedInt(packet.payload[pos:])
			affectedRows := int(l)

			i, _, _ := LengthEncodedInt(packet.payload[pos+n:])
			lastInsertId := int(i)

			f := "%s Effect Row:%s,Last Insert Id:%s"
			msg += fmt.Sprintf(f, OkPacket, strconv.Itoa(affectedRows), strconv.Itoa(lastInsertId))

		default:
			return
	}

	fmt.Println(msg)
}

func (stm *stream) resolveClientPacket(packet *packet) {

	var msg = packetFlowInfo(packet.net, packet.transport)
	switch packet.payload[0] {

	case COM_INIT_DB:

		msg += fmt.Sprintf("USE %s;\n", packet.payload[1:])
	case COM_DROP_DB:

		msg += fmt.Sprintf("Drop DB %s;\n", packet.payload[1:])
	case COM_CREATE_DB:

		statement := string(packet.payload[1:])
		msg += fmt.Sprintf("%s %s", ComQueryRequestPacket, statement)
	case COM_QUERY:

		statement := string(packet.payload[1:])
		msg += fmt.Sprintf("%s %s", ComQueryRequestPacket, statement)

		fmt.Println(msg)

		msg = stm.readResult(packet.seq, false)
	case COM_STMT_PREPARE:

		serverPacket := stm.findStmtPacket(stm.packets, packet.seq+1)
		if serverPacket == nil {
			log.Println("ERR : Not found stm packet")
			return
		}

		//fetch stm id
		stmtID := binary.LittleEndian.Uint32(serverPacket.payload[1:5])
		stmt := &Stmt{
			ID:    stmtID,
			Query: string(packet.payload[1:]),
		}

		//record stm sql
		stm.stmtMap[stmtID] = stmt
		stmt.FieldCount = binary.LittleEndian.Uint16(serverPacket.payload[5:7])
		stmt.ParamCount = binary.LittleEndian.Uint16(serverPacket.payload[7:9])
		stmt.Args       = make([]interface{}, stmt.ParamCount)

		msg += PreparePacket+stmt.Query
	case COM_STMT_SEND_LONG_DATA:

		stmtID := binary.LittleEndian.Uint32(packet.payload[1:5])
		paramId := binary.LittleEndian.Uint16(packet.payload[5:7])
		stmt, _ := stm.stmtMap[stmtID]

		if stmt.Args[paramId] == nil {
			stmt.Args[paramId] = packet.payload[7:]
		} else {
			if b, ok := stmt.Args[paramId].([]byte); ok {
				b = append(b, packet.payload[7:]...)
				stmt.Args[paramId] = b
			}
		}
		return
	case COM_STMT_RESET:

		stmtID := binary.LittleEndian.Uint32(packet.payload[1:5])
		stmt, _ := stm.stmtMap[stmtID]
		stmt.Args = make([]interface{}, stmt.ParamCount)
		return
	case COM_STMT_EXECUTE:

		var pos = 1
		stmtID := binary.LittleEndian.Uint32(packet.payload[pos : pos+4])
		pos += 4
		var stmt *Stmt
		var ok bool
		if stmt, ok = stm.stmtMap[stmtID]; ok == false {
			log.Println("ERR : Not found stm id", stmtID)
			return
		}

		//params
		pos += 5
		if stmt.ParamCount > 0 {

			//（Null-Bitmap，len = (paramsCount + 7) / 8 byte）
			step := int((stmt.ParamCount + 7) / 8)
			nullBitmap := packet.payload[pos : pos+step]
			pos += step

			//Parameter separator
			flag := packet.payload[pos]

			pos++

			var pTypes  []byte
			var pValues []byte

			//if flag == 1
			//n （len = paramsCount * 2 byte）
			if flag == 1 {
				pTypes = packet.payload[pos : pos+int(stmt.ParamCount)*2]
				pos += int(stmt.ParamCount) * 2
				pValues = packet.payload[pos:]
			}

			//bind params
			err := stmt.BindArgs(nullBitmap, pTypes, pValues)
			if err != nil {
				log.Println("ERR : Could not bind params", err)
			}
		}
		msg += string(stmt.WriteToText())

		fmt.Println(msg)

		msg = stm.readResult(packet.seq, true)
	default:
		return
	}

	fmt.Println(msg)
}

