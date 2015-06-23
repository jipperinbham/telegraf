package rethinkdb

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/influxdb/telegraf/plugins"

	"github.com/dancannon/gorethink"
)

type RethinkDB struct {
	Servers []string

	session      *gorethink.Session
	clusterStats clusterStats
}

var sampleConfig = `
# An array of address to gather stats about. Specify an ip on hostname
# with optional port. ie localhost, 10.10.3.33:18832, etc.
#
# If no servers are specified, then localhost is used as the host.
servers = ["localhost"]`

func (r *RethinkDB) SampleConfig() string {
	return sampleConfig
}

func (r *RethinkDB) Description() string {
	return "Read metrics from one or many RethinkDB servers"
}

// Reads stats from all configured servers accumulates stats.
// Returns one of the errors encountered while gather stats (if any).
func (r *RethinkDB) Gather(acc plugins.Accumulator) error {
	if len(r.Servers) == 0 {
		url := &url.URL{
			Host: ":28015",
		}
		r.gatherServer(url, acc)
		return nil
	}

	var wg sync.WaitGroup

	var outerr error

	for _, serv := range r.Servers {
		u, err := url.Parse(serv)
		if err != nil {
			return fmt.Errorf("Unable to parse to address '%s': %s", serv, err)
		} else if u.Scheme == "" {
			// fallback to simple string based address (i.e. "10.0.0.1:10000")
			u.Host = serv
		}
		wg.Add(1)
		go func(serv string) {
			defer wg.Done()
			outerr = r.gatherServer(u, acc)
		}(serv)
	}
	return outerr
}

type clusterStats struct {
	Engine struct {
		ClientConns  int `gorethink:"client_connections"`
		ClientActive int `gorethink:"clients_active"`
		Qps          int `gorethink:"queries_per_sec"`
		Rps          int `gorethink:"read_docs_per_sec"`
		Wps          int `gorethink:"written_docs_per_sec"`
	} `gorethink:"query_engine"`
}

func (r *RethinkDB) gatherServer(serv *url.URL, acc plugins.Accumulator) error {
	var err error
	r.session, err = gorethink.Connect(gorethink.ConnectOpts{
		Address: serv.Host,
	})
	if err != nil {
		return fmt.Errorf("Unable to connect to RethinkDB, %s\n", err.Error())
	}
	if err := r.validateVersion(); err != nil {
		return fmt.Errorf("Failed version validation, %s\n", err.Error())
	}

	tags := map[string]string{"host": serv.Host}
	if err := r.addClusterStats(tags, acc); err != nil {
		return fmt.Errorf("Error adding cluster stats, %s\n", err.Error())
	}
	return nil
}

func (r *RethinkDB) validateVersion() error {
	cursor, err := gorethink.DB("rethinkdb").Table("server_status").Run(r.session)
	if err != nil {
		return err
	}

	if cursor.IsNil() {
		return errors.New("could not determine the RethinkDB server version: no rows returned from the server_status table")
	}

	serverStatus := struct {
		Process struct {
			Version string `gorethink:"version"`
		} `gorethink:"process"`
	}{}
	if err := cursor.One(&serverStatus); err != nil {
		return errors.New("could not parse server_status document")
	}

	if serverStatus.Process.Version == "" {
		return errors.New("could not determine the RethinkDB server version: process.version key missing")
	}

	versionRegexp := regexp.MustCompile("\\d.\\d.\\d")
	versionString := versionRegexp.FindString(serverStatus.Process.Version)
	if versionString == "" {
		return fmt.Errorf("could not determine the RethinkDB server version: malformed version string (%v)", serverStatus.Process.Version)
	}

	version, err := strconv.Atoi(strings.Split(versionString, "")[0])
	if err != nil || version < 2 {
		return fmt.Errorf("unsupported version %s\n", versionString)
	}
	return nil
}

// func (r *RethinkDB) getServerId() error {
// 	cur, err := gorethink.DB("rethinkdb").Table("server_status").Run(r.session)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

func (r *RethinkDB) addClusterStats(tags map[string]string, acc plugins.Accumulator) error {
	cur, err := gorethink.DB("rethinkdb").Table("stats").Get([]string{"cluster"}).Run(r.session)
	if err != nil {
		return fmt.Errorf("cluster stats query error, %s\n", err.Error())
	}
	if err := cur.One(r.clusterStats); err != nil {
		return fmt.Errorf("failure to parse cluster stats, $s\n", err.Error())
	}

	acc.Add("active_clients", r.clusterStats.Engine.ClientActive, tags)
	acc.Add("clients", r.clusterStats.Engine.ClientConns, tags)
	acc.Add("queries_per_sec", r.clusterStats.Engine.Qps, tags)
	acc.Add("read_docs_per_sec", r.clusterStats.Engine.Rps, tags)
	acc.Add("written_docs_per_sec", r.clusterStats.Engine.Wps, tags)
	return nil
}

func init() {
	plugins.Add("rethinkdb", func() plugins.Plugin {
		return &RethinkDB{}
	})
}
