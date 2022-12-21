// original code: https://github.com/jackc/pglogrepl/blob/master/example/pglogrepl_demo/main.go
package main

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

const debugOutput = false

func main() {
	const outputPlugin = "pgoutput" // https://wiki.postgresql.org/wiki/Logical_Decoding_Plugins
	const slotName = "groceries_pub"

	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	conn, err := pgconn.Connect(ctx, "postgres://postgres:test@localhost:5432?replication=database")
	panicOnErr(err, "failed to connect to PostgreSQL server")
	defer conn.Close(ctx)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	panicOnErr(err, "IdentifySystem failed:")
	// sysident.XLogPos = 22526648

	log("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	panicOnErr(err, "CreateReplicationSlot failed:")

	log("Created temporary replication slot", slotName)

	pluginArguments := []string{
		"proto_version '3'",
		fmt.Sprintf("publication_names '%s'", slotName),
	}
	err = pglogrepl.StartReplication(
		ctx,
		conn, slotName,
		sysident.XLogPos,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments},
	)

	panicOnErr(err, "StartReplication failed")
	log("Logical replication started on slot", slotName)

	clientXLogPos := sysident.XLogPos
	msgParser := newLogicalMsgParser()

	for {
		rawMsg, err := conn.ReceiveMessage(ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		panicOnErr(err, "receive msg")
		data, _ := rawMsg.(*pgproto3.CopyData)

		// https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
		msgIdentifier, msg := data.Data[0], data.Data[1:]
		switch msgIdentifier {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			_ = handleKeepAliveMsg(ctx, conn, msg, clientXLogPos)

		// https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-MESSAGES
		case pglogrepl.XLogDataByteID:
			xld, _ := pglogrepl.ParseXLogData(msg)

			_ = msgParser.Handle(xld.WALData)

			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		}
	}
}

type logicalMsgParser struct {
	relations map[uint32]*pglogrepl.RelationMessage
	typeMap   *pgtype.Map
}

func (p *logicalMsgParser) Handle(walData []byte) error {
	logicalMsg, _ := pglogrepl.Parse(walData)

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		// https://www.postgresql.org/docs/current/protocol-logical-replication.html
		// Every DML message contains a relation OID, identifying the publisher's relation that was acted on.
		// Before the first DML message for a given relation OID, a Relation message will be sent,
		// describing the schema of that relation. Subsequently, a new Relation message will be sent if the
		// relation's definition has changed since the last Relation message was sent for it.
		// (The protocol assumes that the client is capable of remembering this metadata for as many relations as needed.)

		// relations map[uint32]*pglogrepl.RelationMessage
		p.relations[logicalMsg.RelationID] = logicalMsg
	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions.
		// You won't get any events from rolled back transactions.
	case *pglogrepl.CommitMessage:

	case *pglogrepl.InsertMessage:
		dataChange, _ := p.decodeColumnData(logicalMsg.RelationID, logicalMsg.Tuple.Columns)
		logf("INSERT INTO %s.%s: %v", dataChange.Namespace, dataChange.TableName, dataChange.Values)
	case *pglogrepl.UpdateMessage:
		dataChange, _ := p.decodeColumnData(logicalMsg.RelationID, logicalMsg.NewTuple.Columns)
		logf("UPDATE %s.%s: %v", dataChange.Namespace, dataChange.TableName, dataChange.Values)
	case *pglogrepl.DeleteMessage:
		dataChange, _ := p.decodeColumnData(logicalMsg.RelationID, logicalMsg.OldTuple.Columns)
		logf("DELETE FROM %s.%s: %v", dataChange.Namespace, dataChange.TableName, dataChange.Values)
	case *pglogrepl.TruncateMessage:
		logf("Got truncate message: %T", logicalMsg)

	case *pglogrepl.TypeMessage:
		// https://www.postgresql.org/docs/current/protocol-logical-replication.html
		// For a non-built-in type OID, a Type message will be sent before the Relation message,
		// to provide the type name associated with that OID. Thus, a client that needs to specifically identify
		// the types of relation columns should cache the contents of Type messages, and first consult that cache
		// to see if the type OID is defined there. If not, look up the type OID locally.
		logf("Got type message: %T", logicalMsg)
	case *pglogrepl.OriginMessage:
		// Every sent transaction contains zero or more DML messages (Insert, Update, Delete). In case of a cascaded
		// setup it can also contain Origin messages. The origin message indicates that the transaction
		// originated on different replication node. Since a replication node in the scope of logical replication protocol
		// can be pretty much anything, the only identifier is the origin name.
		// It's downstream's responsibility to handle this as needed (if needed).
		// The Origin message is always sent before any DML messages in the transaction.
		logf("Got origin message: %T", logicalMsg)
	default:
		logf("Unknown message type in pgoutput stream: %T", logicalMsg)
	}
	return nil
}

func newLogicalMsgParser() logicalMsgParser {
	return logicalMsgParser{
		relations: map[uint32]*pglogrepl.RelationMessage{},
		typeMap:   pgtype.NewMap(),
	}
}

func (p *logicalMsgParser) decodeColumnData(relationID uint32, columns []*pglogrepl.TupleDataColumn) (rowData, error) {
	rel, ok := p.relations[relationID]
	if !ok {
		return rowData{}, fmt.Errorf("unknown relation ID: %d", relationID)
	}

	values := map[string]interface{}{}
	for idx, col := range columns {
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n': // null
			values[colName] = nil
		case 'u': // unchanged toast
			// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
		case 't': // text
			dataType := rel.Columns[idx].DataType
			dt, _ := p.typeMap.TypeForOID(rel.Columns[idx].DataType)
			val, err := dt.Codec.DecodeValue(p.typeMap, dataType, pgtype.TextFormatCode, col.Data)
			panicOnErr(err, "error decoding column data")
			values[colName] = val
		}
	}

	return rowData{Namespace: rel.Namespace, TableName: rel.RelationName, Values: values}, nil
}

func handleKeepAliveMsg(ctx context.Context, conn *pgconn.PgConn, msg []byte, pos pglogrepl.LSN) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg)
	if err != nil {
		return err
	}
	debug("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

	if pkm.ReplyRequested {
		err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: pos})
		if err != nil {
			return fmt.Errorf("SendStandbyStatusUpdate failed: %v", err)
		}
		debug("Sent Standby status message")
	}
	return nil
}

func decodeTextColumnData(mi *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if dt, ok := mi.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(mi, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

type rowData struct {
	Namespace string
	TableName string
	Values    map[string]any
}

func logf(v string, args ...any) {
	log(fmt.Sprintf(v, args...))
}

func log(a ...any) {
	args := append([]any{fmt.Sprintf("[%s]", time.Now().Format("15:04:05.000"))}, a...)
	fmt.Println(args...)
}

func debug(args ...any) {
	if debugOutput {
		log(args...)
	}
}

func panicOnErr(err error, msg string) {
	if err == nil {
		return
	}
	panic(fmt.Sprintf("%s: %v", msg, err))
}
