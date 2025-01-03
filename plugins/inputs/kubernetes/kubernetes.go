//go:generate ../../../tools/readme_config_includer/generator
package kubernetes

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

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
var invalid_sql_chars, _ = regexp.Compile(`[^_a-z0-9]+`)

const (
	defaultServiceAccountPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
)

type IPAddresses struct {
	IPv4Internal string
	IPv4External string
}

type NodeInfo struct {
	Addresses IPAddresses
	Labels map[string]string
}

// Kubernetes represents the config object for the plugin
type Kubernetes struct {
	URL                     string          `toml:"url"`
	BearerToken             string          `toml:"bearer_token"`
	BearerTokenString       string          `toml:"bearer_token_string" deprecated:"1.24.0;1.35.0;use 'BearerToken' with a file instead"`
	NodeMetricName          string          `toml:"node_metric_name"`
	LabelInclude            []string        `toml:"label_include"`
	LabelExclude            []string        `toml:"label_exclude"`
	ResponseTimeout         config.Duration `toml:"response_timeout"`
	Log                     telegraf.Logger `toml:"-"`
	ConvertLabels           bool            `toml:"convert_labels"`
	IncludeNodeLabels       bool            `toml:"include_node_labels"`
	IncludeNodeExternalIPv4 bool            `toml:"include_node_external_ipv4"`

	tls.ClientConfig

	labelFilter filter.Filter
	httpClient  *http.Client
	Hostname    string
	NodeInfoMap map[string]NodeInfo
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

	if k.IncludeNodeExternalIPv4 {
		k.Log.Debugf("k.NodeExternalIP true")
	}

	k.Hostname = os.Getenv("HOSTNAME")
	k.Log.Debugf("HOSTNAME: %s\n", k.Hostname)
	k.NodeInfoMap = make(map[string]NodeInfo)

	return nil
}

func (k *Kubernetes) Gather(acc telegraf.Accumulator) error {
	k.getNodeInfo()
	if k.URL != "" {
		k.Log.Debugf("Gather() k.URL: %s\n", k.URL)
		acc.AddError(k.gatherSummary(k.URL, acc))
		return nil
	}

	k.Log.Debugf("Gather() k.URL empty\n")
	var wg sync.WaitGroup

	for url := range k.NodeInfoMap{
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			acc.AddError(k.gatherSummary(url, acc))
		}(url)
	}
	wg.Wait()

	return nil
}

func (k *Kubernetes) getNodeInfo() error {

	var nodes *v1.NodeList

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}

	if k.URL != "" {
		k.Log.Debugf("getNodeInfo k.URL not blank, get one host: %s\n", k.Hostname)
		fieldselector := fmt.Sprintf("metadata.name=%s", k.Hostname)
		nodes, err = client.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{
			FieldSelector: fieldselector,
		})
		if err != nil {
			return err
		}
	}else {
		nodes, err = client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
		k.Log.Debugf("getNodeInfo k.URL BLANK , get all nodes\n")
		if err != nil {
			return err
		}
	}	

	for i := range nodes.Items {
		k.Log.Debugf("Walk nodes: %d\n", i)
		n := &nodes.Items[i]
		nodeInfo := NodeInfo{}
		nodeInfo.Addresses = getNodeAddresses(n.Status.Addresses, k.Log)
		k.Log.Debugf("Host: %s, Name: %s, Addresses: %+v\n", k.Hostname, n.Name, nodeInfo.Addresses)
		url := "https://"+nodeInfo.Addresses.IPv4Internal+":10250"
		nodeInfo.Labels = n.GetLabels()
		if nodeInfo.Addresses.IPv4Internal == "" {
			k.Log.Warnf("Unable to get node addresses for Node %q", n.Name)
			continue
		}
		if nodeInfo.Addresses.IPv4External != "" {
				k.Log.Debugf("Host: %s, have v4external: %s\n", k.Hostname, nodeInfo.Addresses.IPv4External)
			if k.IncludeNodeExternalIPv4 {
				k.Log.Debugf("Add  %s to labels\n", nodeInfo.Addresses.IPv4External)
				nodeInfo.Labels["node_external_ipv4"] = nodeInfo.Addresses.IPv4External
			}
		}
		k.Log.Debugf("getNodeInfo(): Host: %s, Node: %s, url: %s, %d labels\n", k.Hostname, n.Name, url, len(nodeInfo.Labels))
		k.NodeInfoMap[url] = nodeInfo
	}
	k.Log.Debugf("getNodeInfo() Done\n")
	return nil
}

func getNodeAddresses(naddresses []v1.NodeAddress, log telegraf.Logger) IPAddresses {
	addresses := IPAddresses{}
    for _, addr := range naddresses {
        if addr.Type == v1.NodeInternalIP {
			if addresses.IPv4Internal != "" {
				continue
			}else {
				addresses.IPv4Internal = addr.Address
			}
        }
		if addr.Type == v1.NodeExternalIP {
			if addresses.IPv4External != "" {
				continue
			}else {
				addresses.IPv4External = addr.Address
			}
		}
    }
    return addresses
}

func (k *Kubernetes) gatherSummary(baseURL string, acc telegraf.Accumulator) error {
	k.Log.Debugf("gatherSummary() baseURL: %s\n", baseURL)
	summaryMetrics := &summaryMetrics{}
	err := k.loadJSON(baseURL+"/stats/summary", summaryMetrics)
	if err != nil {
		return err
	}

	podInfos, err := k.gatherPodInfo(baseURL)
	if err != nil {
		return err
	}
	buildSystemContainerMetrics(summaryMetrics, acc)
	k.buildNodeMetrics(baseURL, summaryMetrics, acc)
	k.buildPodMetrics(summaryMetrics, podInfos, acc)
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

func (k *Kubernetes) buildNodeMetrics (baseURL string, summaryMetrics *summaryMetrics, acc telegraf.Accumulator) {

	tags := map[string]string{
		"node_name": summaryMetrics.Node.NodeName,
	}
	var converted string
	if k.IncludeNodeLabels {
		k.Log.Debugf("Host: %s, k.NodeLabels true, baseURL: %s", k.Hostname, baseURL)	
		labels := k.NodeInfoMap[baseURL].Labels
		k.Log.Debugf("Host: %s, %d labels for baseURL: %s\n", k.Hostname, len(labels), baseURL)
		for labelk, labelv := range labels {
			k.Log.Debugf("buildNodeMetrics(): label: %s -> %s", labelk, labelv)
			if k.labelFilter.Match(labelk) {
				k.Log.Debugf("buildNodeMetrics(): filter matched: %s -> %s", labelk, labelv)
				if k.ConvertLabels {
					converted = invalid_sql_chars.ReplaceAllString(labelk, "_") 
					tags[converted] = labelv
				}else {
					tags[labelk] = labelv
				}
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
	acc.AddFields(k.NodeMetricName, fields, tags)
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

	err = json.NewDecoder(resp.Body).Decode(v)
	if err != nil {
		return fmt.Errorf("error parsing response: %w", err)
	}

	return nil
}

func (k *Kubernetes) buildPodMetrics(summaryMetrics *summaryMetrics, podInfo []item, acc telegraf.Accumulator) {

	var converted string

	for _, pod := range summaryMetrics.Pods {
		podLabels := make(map[string]string)
		containerImages := make(map[string]string)
		for _, info := range podInfo {
			if info.Metadata.Name == pod.PodRef.Name && info.Metadata.Namespace == pod.PodRef.Namespace {
				for _, v := range info.Spec.Containers {
					containerImages[v.Name] = v.Image
				}
				for mk, mv := range info.Metadata.Labels {
					if k.labelFilter.Match(mk) {
						podLabels[mk] = mv
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
			for ik, iv := range containerImages {
				if ik == container.Name {
					tags["image"] = iv
					tok := strings.Split(iv, ":")
					if len(tok) == 2 {
						tags["version"] = tok[1]
					}
				}
			}
			for pk, pv := range podLabels {
				if k.ConvertLabels {
					converted = invalid_sql_chars.ReplaceAllString(pk, "_") 
					tags[converted] = pv
				}else {
					tags[pk] = pv
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
			for pk, pv := range podLabels {
				tags[pk] = pv
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
		for pk, pv := range podLabels {
			tags[pk] = pv
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
