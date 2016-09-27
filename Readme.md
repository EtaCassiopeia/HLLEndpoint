protoc -Isrc/main/protobuf --java_out=src/main/java src/main/protobuf/HLLService.proto

create 'hlltest', 'CF',{SPLITS => ['f','o']}

To get the region info about the table, you need to scan hbase:meta table.

 scan 'hbase:meta',{FILTER=>"PrefixFilter('hllTable')"}

info:regioninfo - This qualifier contains STARTKEY and ENDKEY.

info:server - This qualifier contains region server details


    <property>
		<name>hbase.coprocessor.user.region.classes</name>
		<value>com.github.etacassiopeia.hbase.endpoint.HLLEndpoint</value>
	</property>