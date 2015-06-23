package all

import (
	_ "github.com/influxdb/telegraf/plugins/mysql"
	_ "github.com/influxdb/telegraf/plugins/postgresql"
	_ "github.com/influxdb/telegraf/plugins/redis"
	_ "github.com/influxdb/telegraf/plugins/system"
	_ "github.com/jipperinbham/telegraf/plugings/rethinkdb"
)
