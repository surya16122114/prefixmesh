package directory

import "github.com/prometheus/client_golang/prometheus"

// Collector exports the control-plane view: the epoch is the cluster's
// logical clock, so a dashboard panel of pm_directory_ring_epoch tells the
// whole membership story at a glance.
type Collector struct {
	s       *Server
	epoch   *prometheus.Desc
	members *prometheus.Desc
	applied *prometheus.Desc
}

func NewCollector(s *Server) *Collector {
	return &Collector{
		s:       s,
		epoch:   prometheus.NewDesc("pm_directory_ring_epoch", "Current ring epoch.", nil, nil),
		members: prometheus.NewDesc("pm_directory_members", "Live cache-node members.", nil, nil),
		applied: prometheus.NewDesc("pm_directory_log_applied", "Paxos log slots applied.", nil, nil),
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.epoch
	ch <- c.members
	ch <- c.applied
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ring := c.s.sm.Ring()
	ch <- prometheus.MustNewConstMetric(c.epoch, prometheus.GaugeValue, float64(ring.Epoch))
	ch <- prometheus.MustNewConstMetric(c.members, prometheus.GaugeValue, float64(len(ring.Nodes)))
	ch <- prometheus.MustNewConstMetric(c.applied, prometheus.CounterValue, float64(c.s.node.Log.Applied()))
}
