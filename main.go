package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
)

const debugOutput = true

func main() {
	const outputPlugin = "pgoutput" // https://wiki.postgresql.org/wiki/Logical_Decoding_Plugins
	const slotName = "groceries_pub"

	ctx := context.Background()

	conn, err := pgconn.Connect(ctx, "postgres://postgres:test@localhost:5432?replication=database")
	panicOnErr(err, "failed to connect to PostgreSQL server")
	defer conn.Close(ctx)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	panicOnErr(err, "IdentifySystem failed:")
	sysident.XLogPos = 22526648

	log("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	panicOnErr(err, "CreateReplicationSlot failed:")

	log("Created temporary replication slot", slotName)
	pluginArguments := []string{
		"proto_version '1'",
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
	relations := map[uint32]*pglogrepl.RelationMessage{}
	connInfo := pgtype.NewConnInfo()

	for {
		rawMsg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			panicOnErr(err, "ReceiveMessage failed")
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			logf("received Postgres WAL error: %+v", errMsg)
			continue
		}

		data, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			logf("Received unexpected message: %T", rawMsg)
			continue
		}

		// https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
		msgIdentifier, msg := data.Data[0], data.Data[1:]
		switch msgIdentifier {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			err := handleKeepAliveMsg(ctx, conn, msg, clientXLogPos)
			panicOnErr(err, "ParsePrimaryKeepaliveMessage failed")

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg)
			panicOnErr(err, "parse xlogData")
			debug("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", string(xld.WALData))

			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			panicOnErr(err, "parse logical replication message")

			debug("Receive a logical replication message", logicalMsg.Type())
			switch logicalMsg := logicalMsg.(type) {
			case *pglogrepl.RelationMessage:
				relations[logicalMsg.RelationID] = logicalMsg
				log("Got relation Message", logicalMsg)
			case *pglogrepl.BeginMessage:
				// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.
			case *pglogrepl.CommitMessage:
			case *pglogrepl.InsertMessage:
				rel, ok := relations[logicalMsg.RelationID]
				if !ok {
					log("unknown relation ID:", logicalMsg.RelationID)
					continue
				}

				values := map[string]interface{}{}
				for idx, col := range logicalMsg.Tuple.Columns {
					colName := rel.Columns[idx].Name
					switch col.DataType {
					case 'n': // null
						values[colName] = nil
					case 'u': // unchanged toast
						// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
					case 't': // text
						val, err := decodeTextColumnData(connInfo, col.Data, rel.Columns[idx].DataType)
						panicOnErr(err, "error decoding column data")
						values[colName] = val
					}
				}
				logf("INSERT INTO %s.%s: %v", rel.Namespace, rel.RelationName, values)

			case *pglogrepl.UpdateMessage:
				rel, ok := relations[logicalMsg.RelationID]
				if !ok {
					log("unknown relation ID update %d", logicalMsg.RelationID)
					continue
				}

				logf("Got update message %T: %T", rel, logicalMsg)
			case *pglogrepl.DeleteMessage:
				rel, ok := relations[logicalMsg.RelationID]
				if !ok {
					logf("unknown relation ID %d", logicalMsg.RelationID)
					continue
				}

				logf("Got delete message for rel %T: %T", rel, logicalMsg)
			case *pglogrepl.TruncateMessage:
				logf("Got truncate message: %T", logicalMsg)

			case *pglogrepl.TypeMessage:
				logf("Got type message: %T", logicalMsg)
			case *pglogrepl.OriginMessage:
				logf("Got truncate message: %T", logicalMsg)
			default:
				logf("Unknown message type in pgoutput stream: %T", logicalMsg)
			}

			clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
		default:
			logf("Unknown data type in pgoutput stream: %v", data.Data[0])
		}
	}
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

func decodeTextColumnData(ci *pgtype.ConnInfo, data []byte, dataType uint32) (interface{}, error) {
	var decoder pgtype.TextDecoder
	if dt, ok := ci.DataTypeForOID(dataType); ok {
		decoder, ok = dt.Value.(pgtype.TextDecoder)
		if !ok {
			decoder = &pgtype.GenericText{}
		}
	} else {
		decoder = &pgtype.GenericText{}
	}
	if err := decoder.DecodeText(ci, data); err != nil {
		return nil, err
	}
	return decoder.(pgtype.Value).Get(), nil
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
