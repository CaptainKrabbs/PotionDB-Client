module potionDB_client

go 1.22

toolchain go1.24.4

require (
	github.com/antlr/antlr4/runtime/Go/antlr/v4 v4.0.0-20230321174746-8dcc6526cfb1
	google.golang.org/protobuf v1.36.6
	potionDB/crdt v0.0.0
	potionDB/potionDB v0.0.0-00010101000000-000000000000
	sqlToKeyValue v0.0.0
)

require (
	github.com/AndreRijo/go-tools v0.0.0-20250702122434-f5d7580301a7 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/streadway/amqp v1.1.0 // indirect
	github.com/twmb/murmur3 v1.1.5 // indirect
	golang.org/x/exp v0.0.0-20220722155223-a9213eeb770e // indirect
	potionDB/shared v0.0.0 // indirect
	tpch_data_processor v0.0.0 // indirect
)

replace potionDB/potionDB => ../potionDB/potionDB

replace potionDB/shared => ../potionDB/shared

replace potionDB/crdt => ../potionDB/crdt

replace tpch_data_processor => ../tpch_data_processor

replace sqlToKeyValue => ../sqlToKeyValue

replace gotools => ../goTools
