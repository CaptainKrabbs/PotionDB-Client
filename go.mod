module potionDB_client

go 1.16

require (
	github.com/antlr/antlr4/runtime/Go/antlr/v4 v4.0.0-20230321174746-8dcc6526cfb1
	google.golang.org/protobuf v1.34.2
	potionDB/crdt v0.0.0
	potionDB/potionDB v0.0.0-00010101000000-000000000000
	sqlToKeyValue v0.0.0
)

replace potionDB/potionDB => ../potionDB/potionDB

replace potionDB/shared => ../potionDB/shared

replace potionDB/crdt => ../potionDB/crdt

replace tpch_data_processor => ../tpch_data_processor

replace sqlToKeyValue => ../sqlToKeyValue

replace gotools => ../goTools
