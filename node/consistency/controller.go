package consistency

import (
	"fmt"
)

func (c *consistency) RecordLatency(latency int64) {
	if err := c.histogram.RecordValue(latency); err != nil {
		fmt.Print("unable to record latency")
	}
}
