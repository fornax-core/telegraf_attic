//go:generate ../../../tools/readme_config_includer/generator
package kubernetes

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
	"regexp"
	"sync"
	"bytes"
	"io"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/filter"
	"github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/inputs"
)

//go:embed sample.conf
var sampleConfig string
var ipRegex = regexp.MustCompile("https://([0-9.]+):[0-9]+")
var invalid_sql_chars, _ = regexp.Compile(`[^_a-z0-9]+`)

const (
	defaultServiceAccountPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

// Kubernetes represents the config object for the plugin
type Kubernetes struct {
	URL               string          `toml:"url"`
	BearerToken       string          `toml:"bearer_token"`
	BearerTokenString string          `toml:"bearer_token_string" deprecated:"1.24.0;1.35.0;use 'BearerToken' with a file instead"`
	NodeMetricName    string          `toml:"node_metric_name"`
	LabelInclude      []string        `toml:"label_include"`
	LabelExclude      []string        `toml:"label_exclude"`
	ResponseTimeout   config.Duration `toml:"response_timeout"`
	Log               telegraf.Logger `toml:"-"`
	ConvertLabels     bool            `toml:"convert_labels"`
	NodeLabels        bool            `toml:"node_labels"`

	tls.ClientConfig

	labelFilter filter.Filter
	httpClient  *http.Client
}

type NodeInfo struct {
	Node *v1.Node
	URL string
	Labels map[string]string
}

func newNodeInfo(node *v1.Node, url string, labels map[string]string) NodeInfo {
	n := NodeInfo{Node: node, URL: url, Labels: labels}
	return n
}

func (*Kubernetes) SampleConfig() string {
	return sampleConfig
}

func (k *Kubernetes) Init() error {
	// If neither are provided, use the default service account.
	if k.BearerToken == "" && k.BearerTokenString == "" {
		k.BearerToken = defaultServiceAccountPath
	}

	labelFilter, err := filter.NewIncludeExcludeFilter(k.LabelInclude, k.LabelExclude)
	if err != nil {
		return err
	}
	k.labelFilter = labelFilter

	if k.URL == "" {
		k.InsecureSkipVerify = true
	}

	if k.NodeMetricName == "" {
		k.NodeMetricName = "kubernetes_node"
	}

	if k.ConvertLabels {
		k.Log.Debugf("k.ConvertLabels true")
	}

	if k.NodeLabels {
		k.Log.Debugf("k.NodeLabels true")
	}

	k.Log.Debugf("k.Init() k = %+v", k)
	return nil
}

func (k *Kubernetes) Gather(acc telegraf.Accumulator) error {

	nodeInfoMap := make(map[string]NodeInfo)

	kube_svc_host := os.Getenv("KUBERNETES_SERVICE_HOST")

	if kube_svc_host != "" {

		nodes, err := getNodes()
		if err != nil {
			return err
		}

		nodeInfoMap, err = getNodeInfoMap(nodes, k.Log)

		if err != nil {
			return err
		}
	}

	if k.URL != "" {
		k.Log.Debugf("k.URL: %s", k.URL)
		name := urlToName(k.URL, k.Log)
		node, ok := nodeInfoMap[name]
		if ok {
			labels := node.Labels
			acc.AddError(k.gatherSummary(k.URL, labels, acc, k.Log))
		}else {
			labels := make(map[string]string)
			acc.AddError(k.gatherSummary(k.URL, labels, acc, k.Log))
		}
		return nil
	}else {
		k.Log.Info("k.URL is empty")
	}

	var wg sync.WaitGroup

	for _, ni := range nodeInfoMap {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			acc.AddError(k.gatherSummary(ni.URL, ni.Labels, acc, k.Log))
		}(ni.URL)
	}
	wg.Wait()

	return nil
}

func getNodes() (*v1.NodeList, error) {

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	nodes, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})

	if err != nil {
		//panic(err.Error())
		return nil, err
	}
	return nodes, nil
}

func getNodeInfoMap(nl *v1.NodeList, log telegraf.Logger) (map[string]NodeInfo, error) {

	nodeInfoMap := make(map[string]NodeInfo)

	for i := range nl.Items {
		n := &nl.Items[i]
		name := n.Name
		log.Debugf("getNodeInfoList(), node: %s", name)

		address := getNodeAddress(n.Status.Addresses)
		if address == "" {
			log.Warnf("Unable to node addresses for Node %q", n.Name)
			continue
		}
		nodeurl := "https://"+address+":10250"
		labels := n.GetLabels()
		log.Debugf("getNodeInfoList(), %d labels found", len(labels))
		for k, v := range labels {
			log.Debugf("getNodeInfoList(): label: %s -> %s", k, v)
		}
		ni := newNodeInfo(n, nodeurl, labels)
		nodeInfoMap[name] = ni
	}

	return nodeInfoMap, nil
}

// Prefer internal addresses, if none found, use ExternalIP
func getNodeAddress(addresses []v1.NodeAddress) string {
	extAddresses := make([]string, 0)
	for _, addr := range addresses {
		if addr.Type == v1.NodeInternalIP {
			return addr.Address
		}
		extAddresses = append(extAddresses, addr.Address)
	}

	if len(extAddresses) > 0 {
		return extAddresses[0]
	}
	return ""
}

func (k *Kubernetes) gatherSummary(baseURL string, labels map[string]string, acc telegraf.Accumulator, log telegraf.Logger) error {
	summaryMetrics := &summaryMetrics{}
	err := k.loadJSON(baseURL+"/stats/summary", summaryMetrics)
	if err != nil {
		return err
	}
	log.Debugf("gatherSummary(): loadJSON(%s)", (baseURL+"/stats/summary"))

	podInfos, err := k.gatherPodInfo(baseURL)
	if err != nil {
		return err
	}
	buildSystemContainerMetrics(summaryMetrics, acc)
	buildNodeMetrics(summaryMetrics, labels, k.labelFilter, acc, k.NodeMetricName, log, k.ConvertLabels)
	buildPodMetrics(summaryMetrics, podInfos, k.labelFilter, acc, log, k.ConvertLabels)
	return nil
}

func buildSystemContainerMetrics(summaryMetrics *summaryMetrics, acc telegraf.Accumulator) {
	for _, container := range summaryMetrics.Node.SystemContainers {
		tags := map[string]string{
			"node_name":      summaryMetrics.Node.NodeName,
			"container_name": container.Name,
		}
		fields := make(map[string]interface{})
		fields["cpu_usage_nanocores"] = container.CPU.UsageNanoCores
		fields["cpu_usage_core_nanoseconds"] = container.CPU.UsageCoreNanoSeconds
		fields["memory_usage_bytes"] = container.Memory.UsageBytes
		fields["memory_working_set_bytes"] = container.Memory.WorkingSetBytes
		fields["memory_rss_bytes"] = container.Memory.RSSBytes
		fields["memory_page_faults"] = container.Memory.PageFaults
		fields["memory_major_page_faults"] = container.Memory.MajorPageFaults
		fields["rootfs_available_bytes"] = container.RootFS.AvailableBytes
		fields["rootfs_capacity_bytes"] = container.RootFS.CapacityBytes
		fields["logsfs_available_bytes"] = container.LogsFS.AvailableBytes
		fields["logsfs_capacity_bytes"] = container.LogsFS.CapacityBytes
		acc.AddFields("kubernetes_system_container", fields, tags)
	}
}

func buildNodeMetrics(summaryMetrics *summaryMetrics,
						labels map[string]string,
						labelFilter filter.Filter,
						acc telegraf.Accumulator,
						metricName string, log telegraf.Logger,
						convert bool) {

	var converted string

	tags := map[string]string{
		"node_name": summaryMetrics.Node.NodeName,
	}
	log.Debugf("buildNodeMetrics(): got nodename: %s from summaryMetrics", summaryMetrics.Node.NodeName)

	for k, v := range labels {
		log.Debugf("buildNodeMetrics(): label: %s -> %s", k, v)
		if labelFilter.Match(k) {
			log.Debugf("buildNodeMetrics(): filter matched: %s -> %s", k, v)
			if convert {
				converted = invalid_sql_chars.ReplaceAllString(k, "_") 
				tags[converted] = v
			}else {
				tags[k] = v
			}
		}
	}

	fields := make(map[string]interface{})
	fields["cpu_usage_nanocores"] = summaryMetrics.Node.CPU.UsageNanoCores
	fields["cpu_usage_core_nanoseconds"] = summaryMetrics.Node.CPU.UsageCoreNanoSeconds
	fields["memory_available_bytes"] = summaryMetrics.Node.Memory.AvailableBytes
	fields["memory_usage_bytes"] = summaryMetrics.Node.Memory.UsageBytes
	fields["memory_working_set_bytes"] = summaryMetrics.Node.Memory.WorkingSetBytes
	fields["memory_rss_bytes"] = summaryMetrics.Node.Memory.RSSBytes
	fields["memory_page_faults"] = summaryMetrics.Node.Memory.PageFaults
	fields["memory_major_page_faults"] = summaryMetrics.Node.Memory.MajorPageFaults
	fields["network_rx_bytes"] = summaryMetrics.Node.Network.RXBytes
	fields["network_rx_errors"] = summaryMetrics.Node.Network.RXErrors
	fields["network_tx_bytes"] = summaryMetrics.Node.Network.TXBytes
	fields["network_tx_errors"] = summaryMetrics.Node.Network.TXErrors
	fields["fs_available_bytes"] = summaryMetrics.Node.FileSystem.AvailableBytes
	fields["fs_capacity_bytes"] = summaryMetrics.Node.FileSystem.CapacityBytes
	fields["fs_used_bytes"] = summaryMetrics.Node.FileSystem.UsedBytes
	fields["runtime_image_fs_available_bytes"] = summaryMetrics.Node.Runtime.ImageFileSystem.AvailableBytes
	fields["runtime_image_fs_capacity_bytes"] = summaryMetrics.Node.Runtime.ImageFileSystem.CapacityBytes
	fields["runtime_image_fs_used_bytes"] = summaryMetrics.Node.Runtime.ImageFileSystem.UsedBytes
	acc.AddFields(metricName, fields, tags)
}

func (k *Kubernetes) gatherPodInfo(baseURL string) ([]item, error) {
	var podAPI pods
	err := k.loadJSON(baseURL+"/pods", &podAPI)
	if err != nil {
		return nil, err
	}
	podInfos := make([]item, 0, len(podAPI.Items))
	podInfos = append(podInfos, podAPI.Items...)
	return podInfos, nil
}

func (k *Kubernetes) loadJSON(url string, v interface{}) error {
	var req, err = http.NewRequest("GET", url, nil)
    var b bytes.Buffer

	if err != nil {
		return err
	}
	var resp *http.Response
	tlsCfg, err := k.ClientConfig.TLSConfig()
	if err != nil {
		return err
	}

	if k.httpClient == nil {
		if k.ResponseTimeout < config.Duration(time.Second) {
			k.ResponseTimeout = config.Duration(time.Second * 5)
		}
		k.httpClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsCfg,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
			Timeout: time.Duration(k.ResponseTimeout),
		}
	}

	if k.BearerToken != "" {
		token, err := os.ReadFile(k.BearerToken)
		if err != nil {
			return err
		}
		k.BearerTokenString = strings.TrimSpace(string(token))
	}
	req.Header.Set("Authorization", "Bearer "+k.BearerTokenString)
	req.Header.Add("Accept", "application/json")
	resp, err = k.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error making HTTP request to %q: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned HTTP status %s", url, resp.Status)
	}

    resp.Body = io.NopCloser(io.TeeReader(resp.Body, &b))

	k.Log.Debugf("loadJSON(): resp.Body: (%+v)", b.String())

	err = json.NewDecoder(resp.Body).Decode(v)
	if err != nil {
		return fmt.Errorf("error parsing response: %w", err)
	}

	return nil
}

func buildPodMetrics(
	summaryMetrics *summaryMetrics, podInfo []item, labelFilter filter.Filter,
	acc telegraf.Accumulator, log telegraf.Logger, convert bool) {

	for _, pod := range summaryMetrics.Pods {

		var converted string
		podLabels := make(map[string]string)
		containerImages := make(map[string]string)
		for _, info := range podInfo {
			if info.Metadata.Name == pod.PodRef.Name && info.Metadata.Namespace == pod.PodRef.Namespace {
				for _, v := range info.Spec.Containers {
					containerImages[v.Name] = v.Image
				}
				for k, v := range info.Metadata.Labels {
					if labelFilter.Match(k) {
						log.Debugf("buildPodMetrics(): filter matched: %s -> %s", k, v)
						podLabels[k] = v
					}
				}
			}
		}

		for _, container := range pod.Containers {
			tags := map[string]string{
				"node_name":      summaryMetrics.Node.NodeName,
				"namespace":      pod.PodRef.Namespace,
				"container_name": container.Name,
				"pod_name":       pod.PodRef.Name,
			}
			for k, v := range containerImages {
				if k == container.Name {
					tags["image"] = v
					tok := strings.Split(v, ":")
					if len(tok) == 2 {
						tags["version"] = tok[1]
					}
				}
			}
			for k, v := range podLabels {
				if convert {
					converted = invalid_sql_chars.ReplaceAllString(k, "_") 
					tags[converted] = v
				}else {
					tags[k] = v
				}
			}
			fields := make(map[string]interface{})
			fields["cpu_usage_nanocores"] = container.CPU.UsageNanoCores
			fields["cpu_usage_core_nanoseconds"] = container.CPU.UsageCoreNanoSeconds
			fields["memory_usage_bytes"] = container.Memory.UsageBytes
			fields["memory_working_set_bytes"] = container.Memory.WorkingSetBytes
			fields["memory_rss_bytes"] = container.Memory.RSSBytes
			fields["memory_page_faults"] = container.Memory.PageFaults
			fields["memory_major_page_faults"] = container.Memory.MajorPageFaults
			fields["rootfs_available_bytes"] = container.RootFS.AvailableBytes
			fields["rootfs_capacity_bytes"] = container.RootFS.CapacityBytes
			fields["rootfs_used_bytes"] = container.RootFS.UsedBytes
			fields["logsfs_available_bytes"] = container.LogsFS.AvailableBytes
			fields["logsfs_capacity_bytes"] = container.LogsFS.CapacityBytes
			fields["logsfs_used_bytes"] = container.LogsFS.UsedBytes
			acc.AddFields("kubernetes_pod_container", fields, tags)
		}

		for _, volume := range pod.Volumes {
			tags := map[string]string{
				"node_name":   summaryMetrics.Node.NodeName,
				"pod_name":    pod.PodRef.Name,
				"namespace":   pod.PodRef.Namespace,
				"volume_name": volume.Name,
			}
			for k, v := range podLabels {
				tags[k] = v
			}
			fields := make(map[string]interface{})
			fields["available_bytes"] = volume.AvailableBytes
			fields["capacity_bytes"] = volume.CapacityBytes
			fields["used_bytes"] = volume.UsedBytes
			acc.AddFields("kubernetes_pod_volume", fields, tags)
		}

		tags := map[string]string{
			"node_name": summaryMetrics.Node.NodeName,
			"pod_name":  pod.PodRef.Name,
			"namespace": pod.PodRef.Namespace,
		}
		for k, v := range podLabels {
			tags[k] = v
		}
		fields := make(map[string]interface{})
		fields["rx_bytes"] = pod.Network.RXBytes
		fields["rx_errors"] = pod.Network.RXErrors
		fields["tx_bytes"] = pod.Network.TXBytes
		fields["tx_errors"] = pod.Network.TXErrors
		acc.AddFields("kubernetes_pod_network", fields, tags)
	}
}

func init() {
	inputs.Add("kubernetes", func() telegraf.Input {
		return &Kubernetes{
			LabelExclude: []string{"*"},
		}
	})
}

func urlToName(url string, log telegraf.Logger) string {

	res := ipRegex.FindStringSubmatch(url)

	log.Debugf("urlToName(): ipRegex returned: %v", res)
	if res != nil {
		if len(res) == 2 {
			parts := strings.Split(res[1], ".")
			name := fmt.Sprintf("ip-%s-%s-%s-%s.ec2.internal",
				parts[0],
				parts[1],
				parts[2],
				parts[3])
			log.Debugf("urlToName(): found Node name: %v", res)

			return name
		}else {
			return ""
		}
	}else {
		return ""
	}
}
