package logstash

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"strings"

	"github.com/fsouza/go-dockerclient"
	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	conn          net.Conn
	route         *router.Route
	containerTags map[string][]string
}

// NewLogstashAdapter creates a LogstashAdapter with UDP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("udp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	conn, err := transport.Dial(route.Address, route.Options)
	if err != nil {
		return nil, err
	}

	return &LogstashAdapter{
		route:         route,
		conn:          conn,
		containerTags: make(map[string][]string),
	}, nil
}

// Get container tags configured with the environment variable LOGSTASH_TAGS
func GetContainerTags(c *docker.Container, a *LogstashAdapter) []string {
	if tags, ok := a.containerTags[c.ID]; ok {
		return tags
	}

	var tags = []string{}
	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_TAGS=") {
			tags = strings.Split(strings.TrimPrefix(e, "LOGSTASH_TAGS="), ",")
			break
		}
	}

	a.containerTags[c.ID] = tags
	return tags
}

func getLabel(labels map[string]string, index string) string {

	if v, ok := labels[index]; ok {
		return v
	}
	return ""

}

func getAllLabels(labels map[string]string) map[string]string {

	l := make(map[string]string)

	for k, v := range labels {
		if strings.HasPrefix(k, "io.rancher.") {
			continue
		}
		l[strings.Replace(k, ".", "_", -1)] = v
	}
	return l
}

func GetRancherInfo(c *docker.Container) *RancherInfo {

	if getLabel(c.Config.Labels, "io.rancher.stack_service.name") == "" {
		return nil
	}
	container := RancherContainer{
		Name:      getLabel(c.Config.Labels, "io.rancher.container.name"),
		IP:        getLabel(c.Config.Labels, "io.rancher.container.ip"),
		UUID:      getLabel(c.Config.Labels, "io.rancher.container.uuid"),
		StartOnce: getLabel(c.Config.Labels, "io.rancher.container.start_once"),
	}

	stackService := getLabel(c.Config.Labels, "io.rancher.stack_service.name")
	splitService := strings.Split(stackService, "/")
	service := ""
	if len(splitService) == 2 {
		service = splitService[1]
	}
	stack := RancherStack{
		Service:     service,
		Name:        getLabel(c.Config.Labels, "io.rancher.stack.name"),
		Full:        stackService,
		Deployement: getLabel(c.Config.Labels, "io.rancher.service.deployment.unit"),
	}
	rancherInfo := RancherInfo{
		Container: container,
		Stack:     stack,
	}
	return &rancherInfo
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {

	for m := range logstream {

		dockerInfo := DockerInfo{
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: m.Container.Config.Hostname,
			Labels:   getAllLabels(m.Container.Config.Labels),
		}

		tags := GetContainerTags(m.Container, a)

		rancherInfo := GetRancherInfo(m.Container)

		var js []byte
		var data map[string]interface{}

		// Parse JSON-encoded m.Data
		if err := json.Unmarshal([]byte(m.Data), &data); err != nil {
			// The message is not in JSON, make a new JSON message.
			msg := LogstashMessage{
				Message: m.Data,
				Docker:  dockerInfo,
				Rancher: rancherInfo,
				Stream:  m.Source,
				Tags:    tags,
			}

			if js, err = json.Marshal(msg); err != nil {
				// Log error message and continue parsing next line, if marshalling fails
				log.Println("logstash: could not marshal JSON:", err)
				continue
			}
		} else {
			// The message is already in JSON, add the docker specific fields.
			data["docker"] = dockerInfo
			data["tags"] = tags
			data["stream"] = m.Source
			data["rancher"] = rancherInfo
			// Return the JSON encoding
			if js, err = json.Marshal(data); err != nil {
				// Log error message and continue parsing next line, if marshalling fails
				log.Println("logstash: could not marshal JSON:", err)
				continue
			}
		}

		// To work with tls and tcp transports via json_lines codec
		js = append(js, byte('\n'))

		if _, err := a.conn.Write(js); err != nil {
			// There is no retry option implemented yet
			log.Fatal("logstash: could not write:", err)
		}
	}
}

type DockerInfo struct {
	Name     string            `json:"name"`
	ID       string            `json:"id"`
	Image    string            `json:"image"`
	Hostname string            `json:"hostname"`
	Labels   map[string]string `json:"labels"`
}

type RancherInfo struct {
	Container RancherContainer `json:"container"`
	Stack     RancherStack     `json:"stack"`
}

type RancherContainer struct {
	Name      string `json:"name"`           // io.rancher.container.name
	UUID      string `json:"uuid"`           // io.rancher.container.uuid
	IP        string `json:"ip"`             // io.rancher.container.ip
	StartOnce string `json:"once,omitempty"` // io.rancher.container.start_once
}

type RancherStack struct {
	Service     string `json:"service"`               // io.rancher.stack_service.name
	Name        string `json:"name"`                  // io.rancher.stack.name
	Full        string `json:"full"`                  // io.rancher.stack_service.name
	Global      string `json:"global,omitempty"`      // io.rancher.scheduler.global
	Deployement string `json:"deployement,omitempty"` // io.rancher.service.deployment.unit
}

// LogstashMessage is a simple JSON input to Logstash.
type LogstashMessage struct {
	Message string       `json:"message"`
	Stream  string       `json:"stream"`
	Docker  DockerInfo   `json:"docker"`
	Rancher *RancherInfo `json:"rancher,omitempty"`
	Tags    []string     `json:"tags"`
}
